import { DatabaseEvents, GenericDatabase } from "./GenericDatabase";
import { createNanoEvents, Unsubscribe } from "nanoevents";
import { Collection, Db, MongoClient, MongoClientOptions, ObjectId } from "mongodb";
import winston from "winston";
import { tryApplyTrait } from "../healthcheck/Healthcheck";
import log from "../logging/Log";

export function bindClientToHealthcheck(client: MongoClient){
    client.on('close', () => tryApplyTrait('database.status', 'unhealthy'));
    client.on('serverHeartbeatSucceeded', () => tryApplyTrait('database.status', 'healthy'));
    client.on('serverHeartbeatFailed', () => tryApplyTrait('database.status', 'unhealthy'));
    client.on('connectionClosed', () => tryApplyTrait('database.status', 'unhealthy'));
    client.on('serverClosed', () => tryApplyTrait('database.status', 'unhealthy'));
    client.on('error', () => tryApplyTrait('database.status', 'unhealthy'));
    client.on('timeout', () => tryApplyTrait('database.status', 'unhealthy'));
    tryApplyTrait('database.status', 'healthy');
}

export type MongoDBConfiguration = {
    username: string,
    password: string,
    uri: string,
    port: number,
    server: string,
    database: string,
    collections: {
        details: string,
        changelog: string,
    },
    settings?: MongoClientOptions,
};

export abstract class GenericMongoDatabase<READ, CREATE, DELETE, UPDATE, REPRESENTATION> implements GenericDatabase<READ, CREATE, DELETE, UPDATE, REPRESENTATION> {

    /**
     * The emitter through which updates about this connection will be sent
     * @private
     */
    private _emitter = createNanoEvents<DatabaseEvents>();

    /**
     * The connection to the MongoDB server
     * @private
     */
    private _client?: MongoClient;

    /**
     * The database we are connected to and on which we are performing operations
     * @private
     */
    private _database?: Db;

    /**
     * The collection storing entries themselves
     * @protected
     */
    protected _details?: Collection;
    /**
     * The collection storing changes to entries
     * @protected
     */
    protected _changelog?: Collection;

    /**
     * The configuration used the construct this database connection
     * @private
     */
    private _configuration?: MongoDBConfiguration;

    /**
     * If this class currently has an good connection to the database
     * @private
     */
    private _connected = false;

    /**
     * Creates a new database connection from scratch using the configuration settings provided. On successful setup
     * 'ready' event will be emitted and on failure it will emit 'error'
     * @param _configuration the configuration used to produce the database connection
     * @protected
     */
    protected constructor(_configuration: MongoDBConfiguration);
    /**
     * Creates the database from an existing database connection using the collections provided. This will not construct
     * a new database connection and will emit 'ready' as soon as everything is prepared. This should happen
     * asynchronously
     * @param database the database on which this mongo database instance should rest
     * @param collections the collections to be used in the database connection
     * @protected
     */
    protected constructor(database: Db, collections: MongoDBConfiguration['collections']);
    // TODO: remove this shim?
    /**
     * A collective constructor which supports both of the previous two constructors. This provides an easy way to
     * override
     * @param _configurationOrDB either the configuration used to construct the database or the instance to use
     * @param collections the collections to use if a Db instance is provided
     * @protected
     */
    protected constructor(_configurationOrDB: MongoDBConfiguration | Db, collections?: MongoDBConfiguration['collections']);
    protected constructor(
        /**
         * The configuration for connecting to the database which will be used to form the URI string and connection
         * settings
         */
        _configurationOrDB: MongoDBConfiguration | Db,
        collections?: MongoDBConfiguration['collections'],
    ) {
        // TODO fix this hacky as fuck shim to make it work
        if (Object.prototype.hasOwnProperty.call(_configurationOrDB, 'username') && Object.prototype.hasOwnProperty.call(_configurationOrDB, 'collections')) {
            const conf = _configurationOrDB as MongoDBConfiguration;

            log.system.debug('mb:GenericMongoDatabase:constructor','creating database using a new database connection');
            this._configuration = conf;
            const username = encodeURIComponent(this._configuration.username);
            const password = encodeURIComponent(this._configuration.password);
            const uri = encodeURIComponent(this._configuration.uri);
            const server = encodeURIComponent(this._configuration.server);
            const { port } = this._configuration;

            MongoClient.connect(
                `mongodb://${username}:${password}@${uri}:${port}/${server}`,
                {
                    ...(this._configuration.settings ? this._configuration.settings : {}),
                    useNewUrlParser: true,
                    useUnifiedTopology: true,
                    reconnectInterval: 60000,
                    reconnectTries: 100,
                } as MongoClientOptions,
            ).then((client) => {
                this._client = client;
                bindClientToHealthcheck(client);

                this._database = client.db(conf.database);

                this._details = this._database.collection(conf.collections.details);
                this._changelog = this._database.collection(conf.collections.changelog);

                this._connected = true;
                this._emitter.emit('ready');
            }).catch((err: unknown) => {
                this._emitter.emit('error', err);
            });
        } else {
            const db = _configurationOrDB as Db;

            if (!collections) throw new Error('Invalid invocation, collection must be provided');
            log.system.debug('mb:GenericMongoDatabase:constructor','creating database using an existing database connection', { collections });
            // If collections is defined then we need the second constructor where the first param is a db
            this._database = db;
            this._details = db.collection(collections.details);
            this._changelog = db.collection(collections.changelog);

            this._connected = true;
            this._emitter.emit('ready');
        }
    }

    /**
     * Attaches an event listener to the underlying event emitter used by this database handler
     * @param event the event on which this listener should listen
     * @param callback the callback to be executed when the event is emitted
     */
    public on<E extends keyof DatabaseEvents>(
        event: E,
        callback: DatabaseEvents[E],
    ): Unsubscribe {
        return this._emitter.on(event, callback);
    }

    /**
     * Registers an event handler which will be instantly unbound when called and therefore only executed on the first
     * event after this handler is registered
     * @param event the event on which this listener should be registered
     * @param callback the callback to be executed on the first occurrence of this emit
     */
    public once<E extends keyof DatabaseEvents>(
        event: E,
        callback: DatabaseEvents[E],
    ) {
        const unbind = this._emitter.on(event, (...args: any[]) => {
            unbind();

            // @ts-ignore
            callback(...args);
        });

        return unbind;
    }

    /**
     * The function that will actually be called when a create action needs to take place. To avoid doubt that the
     * details and changelog collection is defined, they are passed in directly. These will be asserted by the parent
     * functions to ensure that the database is ready
     * @param create the message requesting the create action
     * @param details the collection in which the actual details of the object should be stored
     * @param changelog the collection in which a change trail should be recorded
     * @protected
     */
    protected abstract createImpl(create: CREATE, details: Collection, changelog: Collection): Promise<string[]>;

    /**
     * The function that will actually be called when a delete action needs to take place. To avoid doubt that the
     * details and changelog collection is defined, they are passed in directly. These will be asserted by the parent
     * functions to ensure that the database is ready
     * @param create the message requesting the delete action
     * @param details the collection in which the actual details of the object should be stored
     * @param changelog the collection in which a change trail should be recorded
     * @protected
     */
    protected abstract deleteImpl(create: DELETE, details: Collection, changelog: Collection): Promise<string[]>;

    /**
     * The function that will actually be called when an update action needs to take place. To avoid doubt that the
     * details and changelog collection is defined, they are passed in directly. These will be asserted by the parent
     * functions to ensure that the database is ready
     * @param create the message requesting the update action
     * @param details the collection in which the actual details of the object should be stored
     * @param changelog the collection in which a change trail should be recorded
     * @protected
     */
    protected abstract updateImpl(create: UPDATE, details: Collection, changelog: Collection): Promise<string[]>;

    /**
     * The function that will actually be called when a query action needs to take place. To avoid doubt that the
     * details and changelog collection is defined, they are passed in directly. These will be asserted by the parent
     * functions to ensure that the database is ready
     * @param create the message requesting the query action
     * @param details the collection in which the actual details of the object should be stored
     * @param changelog the collection in which a change trail should be recorded
     * @protected
     */
    protected abstract queryImpl(create: READ, details: Collection, changelog: Collection): Promise<REPRESENTATION[]>;

    /**
     * Handles a create message by asserting the database exists and is connected and that both collections are
     * valid. If not it will throw an error, otherwise {@link createImpl} will be called
     * @param create the create instruction
     */
    public create = async (create: CREATE): Promise<string[]> => {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.createImpl(create, this._details, this._changelog);
    }

    /**
     * Handles a delete message by asserting the database exists and is connected and that both collections are
     * valid. If not it will throw an error, otherwise {@link deleteImpl} will be called
     * @param del the delete instruction
     */
    async delete(del: DELETE): Promise<string[]> {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.deleteImpl(del, this._details, this._changelog);
    }

    /**
     * Handles a query message by asserting the database exists and is connected and that both collections are
     * valid. If not it will throw an error, otherwise {@link queryImpl} will be called
     * @param query the query instruction
     */
    async query(query: READ): Promise<REPRESENTATION[]> {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.queryImpl(query, this._details, this._changelog);
    }

    /**
     * Handles a update message by asserting the database exists and is connected and that both collections are
     * valid. If not it will throw an error, otherwise {@link updateImpl} will be called
     * @param update the update instruction
     */
    async update(update: UPDATE): Promise<string[]> {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.updateImpl(update, this._details, this._changelog);
    }

    /**
     * Log a message to the changelog. This is a fail-fast-and-fail-silently function so if the changelog is not defined
     * it will just return without providing any error. Otherwise it will insert the ID, action and if provided any
     * additional properties. The timestamp is also included set to the current date as provided via {@link Date.now()}
     * @param id the id of the entity which has been changed
     * @param action the action which occurred on the entity
     * @param additional and additional properties which should be added to the change request
     * @protected
     */
    protected async log(id: string, action: string, additional: Record<string, any> = {}) {
        if (!this._changelog) return;

        try {
            await this._changelog.insertOne({
                ...additional,
                id,
                action,
                timestamp: Math.floor(Date.now() / 1000),
            });
        } catch (e) {
            console.warn('Failed to save changelog');
        }
    }

}
