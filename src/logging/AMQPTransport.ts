import {Transport} from "./Log";
import {Channel, connect, Connection, Options} from "amqplib";

type AMQPConfig = {
    connection: Connection,
} | Options.Connect;

/**
 * Pushes all log messages to `uems-log` in the given rabbitmq system
 * @param config the configuration which should be used to generate the connection
 * @constructor
 */
const AMQPTransport: ((config: AMQPConfig) => Promise<Transport>) = async (config: AMQPConfig) => {
    let channel: Channel;

    // Either use the existing connection or generate a new one based on which type of
    // configuration is in use
    if ('connection' in config) {
        channel = await config.connection.createChannel();
    } else {
        const connection = await connect(config);
        channel = await connection.createChannel();
    }

    // Create the exchange and on transport push every message to it
    await channel.assertExchange('uems-log', 'fanout', {});
    return (traceID, module, origin, level, message, data) => {
        channel.publish('uems-log', '', Buffer.from(JSON.stringify({
            traceID, module, origin, level, message, data,
        }), 'utf-8'), {
            expiration: 30000,
        });
    }
}

export default AMQPTransport;