import * as zod from 'zod';
import path from "path";
import * as fs from "fs/promises";
import log, {configure} from "../logging/Log";
import {GenericMongoDatabase} from "../database/GenericMongoDatabase";
import {MongoClient, MongoClientOptions} from "mongodb";
import {tryApplyTrait} from "../healthcheck/Healthcheck";
import {RabbitNetworkHandler} from "../messaging/GenericRabbitNetworkHandler";
import {DataValidator} from "../messaging/AbstractBrokerHandler";
import {Options} from "amqplib";
import AMQPTransport from "../logging/AMQPTransport";
import broker = Configuration.broker;

const appRoot = require('app-root-path');

const DefaultDatabaseValidator = zod.object({
    username: zod.string(),
    password: zod.string(),
    uri: zod.string(),
    port: zod.number(),
    server: zod.string(),
    settings: zod.any().optional(),
});
type DefaultDatabaseConfiguration = zod.infer<typeof DefaultDatabaseValidator>;

export type BrokerShorthandType = {
    generic: never,
    create: never,
    remove: never,
    read: never,
    update: never,
    response: never,
};

export const RabbitMQConfigurationValidator = zod.object({
    options: zod.object({
        protocol: zod.string(),
        hostname: zod.string(),
        port: zod.number(),
        username: zod.string(),
        password: zod.string(),
        locale: zod.string(),
        frameMax: zod.number(),
        heartbeat: zod.number(),
        vhost: zod.string(),
    }).deepPartial(),
    gateway: zod.string(),
    request: zod.string(),
    inbox: zod.string(),
    topics: zod.array(zod.string()),
})
type RabbitMQConfiguration = zod.infer<typeof RabbitMQConfigurationValidator>;

export namespace Configuration {

    export async function logbind(
        module: string,
        broker: RabbitNetworkHandler<any, any, any, any, any, any>
    ) {
        if (!broker.connection) {
            log.auto.system.error('Tried to logbind without a valid connection backing the RabbitMQNetworkHandler');
            throw new Error('Tried to logbind without a valid connection backing the RabbitMQNetworkHandler');
        }

        configure({
            transports: [await AMQPTransport({connection: broker.connection})],
            module: 'gateway',
        }, 'merge');
    }

    export async function broker<T extends {}, K extends keyof T, M extends BrokerShorthandType>(
        configuration: T,
        key: K,
        incoming?: DataValidator,
        outgoing?: DataValidator,
    ): Promise<RabbitNetworkHandler<M['generic'], M['create'], M['remove'], M['read'], M['update'], M['response']>> {
        let configurationParse = RabbitMQConfigurationValidator.safeParse(configuration[key]);
        if (!configurationParse.success) {
            log.auto.system.error('Failed to parse the rabbit mq configuration from config as it did not match the given schema');
            log.auto.system.error(`  Configuration Key = ${key}`);
            configurationParse.error.format()._errors.forEach((error, index) => {
                log.auto.system.error(`  [${index}]: ${error}`);
            });

            throw configurationParse.error;
        }

        const config = configurationParse.data;
        let resolve: undefined | ((value: RabbitNetworkHandler<any, any, any, any, any, any>) => void) = undefined;
        let reject: undefined | ((err: Error) => void) = undefined;

        let promise = new Promise<RabbitNetworkHandler<any, any, any, any, any, any>>((resolveInner, rejectInner) => {
            resolve = resolveInner;
            reject = rejectInner;
        });

        const messenger = new RabbitNetworkHandler(
            config,
            incoming ?? ((data: any) => true),
            outgoing ?? ((data: any) => true),
        );

        const unbind = messenger.once('error', (err) => {
            const converted = {
                ...config,
                options: {...config.options, password: config.options.password ? '[REDACTED]' : undefined}
            };
            log.auto.system.error('Failed to connect to the RabbitMQ broker');
            log.auto.system.error(`  Configuration = ${JSON.stringify(converted)}`);
            log.auto.system.error(`  Raised Error = ${err.message}`, err);

            reject?.(err);
        });

        messenger.once('ready', () => {
            unbind();
            resolve?.(messenger);
        });

        return promise;
    }

    /**
     * Attempts to load a MongoClient from the key in the configuration. This will be validated against
     * {@link DefaultDatabaseValidator} to check that it is valid before it attempts to connect
     * @param configuration the base configuration object
     * @param key the key at which the database config can be found
     */
    export async function database<T extends {}, K extends keyof T>(
        configuration: T,
        key: K,
    ): Promise<MongoClient> {
        let configurationParse = DefaultDatabaseValidator.safeParse(configuration[key]);
        if (!configurationParse.success) {
            log.auto.system.error('Failed to parse the database configuration from config as it did not match the given schema');
            log.auto.system.error(`  Configuration Key = ${key}`);
            configurationParse.error.format()._errors.forEach((error, index) => {
                log.auto.system.error(`  [${index}]: ${error}`);
            });

            throw configurationParse.error;
        }

        const config = configurationParse.data;
        const username = encodeURIComponent(config.username);
        const password = encodeURIComponent(config.password);
        const uri = encodeURIComponent(config.uri);
        const server = encodeURIComponent(config.server);
        const port = config.port;

        try {
            const connection = await MongoClient.connect(
                `mongodb://${username}:${password}@${uri}:${port}/${server}`,
                {
                    ...(config.settings ? config.settings : {}),
                    useNewUrlParser: true,
                    useUnifiedTopology: true,
                    reconnectInterval: 60000,
                    reconnectTries: 100,
                } as MongoClientOptions,
            ).then((client) => {
                client.on('close', () => tryApplyTrait('database.status', 'unhealthy'));
                client.on('serverHeartbeatSucceeded', () => tryApplyTrait('database.status', 'healthy'));
                client.on('serverHeartbeatFailed', () => tryApplyTrait('database.status', 'unhealthy'));
                client.on('connectionClosed', () => tryApplyTrait('database.status', 'unhealthy'));
                client.on('serverClosed', () => tryApplyTrait('database.status', 'unhealthy'));
                client.on('error', () => tryApplyTrait('database.status', 'unhealthy'));
                client.on('timeout', () => tryApplyTrait('database.status', 'unhealthy'));
                client.on('open', () => tryApplyTrait('database.status', 'healthy'));
                tryApplyTrait('database.status', 'healthy');

                return client;
            })

            return connection;
        } catch (err: any) {
            tryApplyTrait('database.status', 'unhealthy');

            log.auto.system.error('Failed to connect to the database');
            log.auto.system.error(`  Username = ${username}`);
            log.auto.system.error(`  Password = [REDACTED]`);
            log.auto.system.error(`  URI = ${uri}`);
            log.auto.system.error(`  Port = ${port}`);
            log.auto.system.error(`  Server = ${server}`);
            log.auto.system.error(`  Settings = ${JSON.stringify(config.settings ?? {})}`);
            log.auto.system.error(`  Raised Error = ${err.message}`, err);

            throw err;
        }
    }

    /**
     * This will attempt to load the configuration, parse it and return it. It will be automatically validated before
     * returning. In the event that its not valid, messages will be logged via {@link log} at the FATAL level and then
     * the error will be re-thrown. This means that you should not need to .catch() this as most logging is done
     * internally, however you should use it for cleanup if required. The module name will be automatically expanded
     * into the environment variable to use for the configuration override. It will become
     * UEMS_{modulename}_CONFIG_LOCATION. Otherwise it will try and load configuration.json from the app route.
     * @param module the name of this module to produce the environment variable
     * @param schema the schema against which the config should be validated
     */
    export async function load<T extends zod.ZodSchema>(
        module: string,
        schema: T,
    ): Promise<zod.infer<T>> {
        const environmentVariable = `UEMS_${module.toUpperCase()}_CONFIG_LOCATION`;
        const fallbackLocation = path.join(appRoot, 'config', 'configuration.json');
        const configLocation = process.env[environmentVariable] ?? fallbackLocation;

        let configurationRaw;
        try {
            configurationRaw = await fs.readFile(configLocation, {encoding: 'utf-8'});
        } catch (e: any) {
            log.auto.system.fatal('Failed to load the configuration from disk');
            log.auto.system.fatal(`  Environment Variable = ${environmentVariable}`);
            log.auto.system.fatal(`  Fallback Location = ${fallbackLocation}`);
            log.auto.system.fatal(`  Configuration Location = ${configLocation}`);
            log.auto.system.fatal(`  Reported Error = ${e.message}`, e);
            throw e;
        }

        let configurationJSON;
        try {
            configurationJSON = JSON.parse(configurationRaw);
        } catch (e: any) {
            log.auto.system.fatal('Failed to parse the configuration from disk as the JSON failed to parse');
            log.auto.system.fatal(`  Reported Error = ${e.message}`, e);
            throw e;
        }

        let configurationParse = schema.safeParse(configurationJSON);
        if (!configurationParse.success) {
            log.auto.system.fatal('Failed to parse the configuration from disk as it did not match the given schema');
            configurationParse.error.format()._errors.forEach((error, index) => {
                log.auto.system.fatal(`  [${index}]: ${error}`);
            });
            throw configurationParse.error;
        }

        return configurationParse.data;
    }
}

export default async function bootstrap<T extends zod.ZodSchema, M extends BrokerShorthandType>(
    module: string,
    schema: T,
    database: string,
    broker: string,
    incoming?: DataValidator,
    outgoing?: DataValidator,
): Promise<[zod.infer<T>, MongoClient, RabbitNetworkHandler<M['generic'], M['create'], M['remove'], M['read'], M['update'], M['response']>]> {
    const setup = await Configuration.load(module, schema);

    if (!Object.prototype.hasOwnProperty.call(setup, database) || !(database in setup))
        throw new Error(`Database key [${database}] was not contained in the loaded configuration`);
    if (!Object.prototype.hasOwnProperty.call(setup, broker) || !(broker in setup))
        throw new Error(`Broker key [${broker}] was not contained in the loaded configuration`);

    const dbp = Configuration.database(setup, database);
    const mqp = Configuration.broker<T, any, M>(setup, broker, incoming, outgoing);
    const [db, mq] = await Promise.all([dbp, mqp]);

    await Configuration.logbind(module, mq);

    return [setup, db, mq];
}