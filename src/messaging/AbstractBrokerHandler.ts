import { constants } from "http2";
import { Channel, connect, Connection, ConsumeMessage, Options } from "amqplib";
import winston from "winston";
import log from "../logging/Log";

export type MessagingConfiguration = {
    options: Options.Connect,
    gateway: string,
    request: string,
    inbox: string,
    topics: string[],
};

let backoffTime = 5000;
let maximumBackoffTime = 120000;

export type ConnectFunction = (url: string | Options.Connect, socketOptions?: any) => Promise<Connection>;
export type DataValidator = (message: any) => boolean | Promise<boolean> | PromiseLike<boolean>;

export abstract class AbstractBrokerHandler {

    /**
     * If this network handler currently has successfully configured connection to the rabbit mq server
     * @private
     */
    private _connected = false;

    /**
     * The open connection to the rabbit mq server
     * @private
     */
    private _connection?: Connection;

    /**
     * The channel on which the responses should be made
     * @private
     */
    private _responseChannel?: Channel;

    /**
     * Handlers current waiting for a ready signal
     * @private
     */
    private _waitingForReady: ((...args: any) => void)[] = [];

    protected constructor(
        /**
         * The configuration used to setup the message broker including the queues names
         */
        private _configuration: MessagingConfiguration,
        /**
         * The validator which should be used to parse incoming messages received from the broker
         * @private
         */
        private _incomingValidator: DataValidator,
        /**
         * The validator which should be used to parse outgoing messages from the clients
         * @private
         */
        private _outgoingValidator: DataValidator,
        /**
         * The function used to connect to the amqplib server. This will be used on reconnect
         * @private
         */
        private connectionMethod?: ConnectFunction,
    ) {
        void this.connect();
    }

    private async connect() {
        ((this.connectionMethod ?? connect) as ConnectFunction)(this._configuration.options).then((connection: Connection) => {
            this._connection = connection;
            return this.setupConnection();
        }).catch((err: unknown) => {
            if (err instanceof Error) {
                log.system.error('mb:AbstractBrokerHandler:connect', 'received an error via catch of amqplib connect', {
                    error: err,
                });

                void this.error(err);
            } else {
                log.system.error('mb:AbstractBrokerHandler:connect',`received an error via catch of amqplib connect but it did not match an instance of 
                an error. The error is logged here and an unknown error is being passed to the event handlers`, {
                    error: err,
                });

                void this.error(new Error('Unknown error on amqplib connection reject'));
            }

            this.connectionErrorHandler(err as Error);
        });
    }

    get connection(): Connection|undefined {
        return this._connection;
    }

    public onReady = (x: ((...args: any) => void)): void => {
        if (this._connected) x();
        else {
            this._waitingForReady.push(x);
        }
    };

    public asyncOnReady() {
        return new Promise<void>((resolve) => {
            this.onReady(resolve);
        });
    }

    /**
     * Logs the error submitted on the amqplib logger and outputs special messages in the case of a connection closing
     * @param err the error raised by the connection
     */
    private connectionErrorHandler = (err: Error) => {
        log.system.error('mb:AbstractBrokerHandler:connectionErrorHandler','an error was raised by the amqplib connection', {
            error: err,
        });

        if (err.message === 'Connection closing') {
            log.system.warn('mb:AbstractBrokerHandler:connectionErrorHandler','connection closing message received, this should be handled by the close handler');
        }

        log.system.warn('mb:AbstractBrokerHandler:connectionErrorHandler',`Trying to reconnect in ${backoffTime}ms`);
        setTimeout(this.connect, Math.min(backoffTime, maximumBackoffTime));
        backoffTime *= 1.2;

        void this.error(err);
    };

    /**
     * Configures the connection the rabbitmq broker by attaching error handlers, message handlers and ensuring that
     * this class is attached to all the channels as defined in the configuration.
     */
    private setupConnection = async () => {
        if (!this._connection) {
            log.system.error('mb:AbstractBrokerHandler:setupConnection','setup connection was called with a falsy connection value. This should not happen!');
            throw new Error('invalid connection parameter');
        }
        log.system.debug('mb:AbstractBrokerHandler:setupConnection','valid connection received, beginning setup and listener attachment');

        this._connection.on('error', this.connectionErrorHandler);
        this._connection.on('close', () => {
            this._connected = false;
            log.system.warn('mb:AbstractBrokerHandler:setupConnection','disconnected from the message broker due to a close event being received on the connection');
            void this.error(Error('disconnected[close]'));
        });

        log.system.debug('mb:AbstractBrokerHandler:setupConnection','event listeners configured');

        // Ensure that the gateway exchange exists
        this._responseChannel = await this._connection.createChannel();
        await this._responseChannel.assertExchange(this._configuration.gateway, 'direct');

        log.system.debug('mb:AbstractBrokerHandler:setupConnection','response exchange created');

        // Ensure the request exchange exists
        const requestChannel = await this._connection.createChannel();
        await requestChannel.assertExchange(this._configuration.request, 'topic', {
            durable: false,
        });

        log.system.debug('mb:AbstractBrokerHandler:setupConnection','request exchange created');

        // Ensure that the inbox queue exists. It shouldn't be exclusive in the case of multiple microservices
        const inbox = await requestChannel.assertQueue(this._configuration.inbox, {
            exclusive: false,
        });

        log.system.debug('mb:AbstractBrokerHandler:setupConnection','inbox created');

        // Bind the queue to all messages with the requested topics. This ensures we only answer messages destined for
        // this service. As this is an async function and we could have many topics, we want to wait until all of them
        // have been bound
        await Promise.all(
            this._configuration.topics.map((topic) => requestChannel.bindQueue(
                inbox.queue,
                this._configuration.request,
                topic,
            )),
        );

        log.system.debug('mb:AbstractBrokerHandler:setupConnection',`bound ${this._configuration.topics.length} topics to the inbox queue`);

        await requestChannel.consume(inbox.queue, this.handleIncoming, {
            noAck: true,
        });

        log.system.debug('mb:AbstractBrokerHandler:setupConnection','consumer attached, ready to receive');

        this._connected = true;
        this._waitingForReady.forEach((e) => e());
        void this.ready();
    };

    /**
     * Handles incoming messages and dispatches them to their listeners on the event and waits for a response
     * @param message the message received from the message broker
     */
    private readonly handleIncoming = async (message: ConsumeMessage | null): Promise<void> => {
        if (message === null) {
            log.system.warn('mb:AbstractBrokerHandler:handleIncoming','received a null message from the inbox queue. ignoring');
            log.system.debug('mb:AbstractBrokerHandler:handleIncoming','this suggests this consumer has been cancelled by RabbitMQ');
            return;
        }

        let content: Record<string, unknown>;

        // Try parsing as JSON but raise an error if that fails, we can only accept perfect messages
        try {
            content = JSON.parse(message.content.toString()) as typeof content;
        } catch (e) {
            log.system.error('mb:AbstractBrokerHandler:handleIncoming',`received an invalid message payload. an error was raised when attempting to parse the 
            content as json`, {
                error: e as unknown,
            });
            return;
        }

        if (!await this._incomingValidator(content)) {
            log.system.error('mb:AbstractBrokerHandler:handleIncoming',`received an invalid message payload. it did not validate against the uemsCommsLib schema 
            definitions for incoming messages`, {
                content,
            });
            return;
        }

        const genericErrorHandler = (err: any) => {
            log.system.debug('mb:AbstractBrokerHandler:error','other handler failed and rejected', {
                error: err as unknown,
            });
            void this.error(new Error('generic handler rejects'));
        };

        if (Object.prototype.hasOwnProperty.call(content, 'msg_intention')) {
            switch (content.msg_intention) {
                case 'CREATE':
                    log.system.debug('mb:AbstractBrokerHandler:handleIncoming','got a create message');
                    Promise.resolve(this.create(content, message.fields.routingKey)).catch(genericErrorHandler);
                    break;
                case 'UPDATE':
                    log.system.debug('mb:AbstractBrokerHandler:handleIncoming','got an update message');
                    Promise.resolve(this.update(content, message.fields.routingKey)).catch(genericErrorHandler);
                    break;
                case 'DELETE':
                    log.system.debug('mb:AbstractBrokerHandler:handleIncoming','got a delete message');
                    Promise.resolve(this.delete(content, message.fields.routingKey)).catch(genericErrorHandler);
                    break;
                case 'READ':
                    log.system.debug('mb:AbstractBrokerHandler:handleIncoming','got a query message');
                    Promise.resolve(this.read(content, message.fields.routingKey)).catch(genericErrorHandler);
                    break;
                default:
                    log.system.debug('mb:AbstractBrokerHandler:handleIncoming','got an unknown message');
                    Promise.resolve(this.other(content, message.fields.routingKey)).catch(genericErrorHandler);
                    break;
            }
        } else {
            Promise.resolve(this.other(content, message.fields.routingKey)).catch(genericErrorHandler);
        }
    };

    protected async send(messageID: number, messageIntention: string, response: any) {
        if (!this._responseChannel) {
            log.system.error('mb:AbstractBrokerHandler:send','got a response but the response channel is undefined?');
            return;
        }

        if (!await this._outgoingValidator(response)) {
            log.system.error('mb:AbstractBrokerHandler:send',`a response was submitted to the handler that did not validate against the provided 
            outgoing validator object. sending an error response to the parent instead`, {
                response: response as unknown,
            });

            this._responseChannel.publish(this._configuration.gateway, '', Buffer.from(JSON.stringify({
                msg_id: messageID,
                msg_intention: messageIntention,
                status: constants.HTTP_STATUS_INTERNAL_SERVER_ERROR,
            })));

            return;
        }

        this._responseChannel.publish(this._configuration.gateway, '', Buffer.from(JSON.stringify(response)));
    }

    protected abstract error(err?: any): void | PromiseLike<void> | Promise<void>;

    protected abstract ready(): void | PromiseLike<void> | Promise<void>;

    protected abstract create(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void>;

    protected abstract delete(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void>;

    protected abstract update(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void>;

    protected abstract read(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void>;

    protected abstract other(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void>;
}
