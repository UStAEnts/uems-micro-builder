import { AbstractBrokerHandler, ConnectFunction, DataValidator, MessagingConfiguration } from "./AbstractBrokerHandler";
import { createNanoEvents, Unsubscribe } from "nanoevents";
import winston from "winston";

const __ = winston.child({ label: __filename });

/**
 * Defines the interface of events dispatched by the message handler. This is used to inform the event handlers
 */
interface RabbitNetworkHandlerEvents<GENERIC,
    CREATE extends GENERIC,
    DELETE extends GENERIC,
    READ extends GENERIC,
    UPDATE extends GENERIC,
    RESPONSE> {
    ready: () => void,
    create: (
        message: CREATE,
        send: (res: RESPONSE) => void,
        routingKey: string,
    ) => void | Promise<void>,
    delete: (
        message: DELETE,
        send: (res: RESPONSE) => void,
        routingKey: string,
    ) => void | Promise<void>,
    query: (
        message: READ,
        send: (res: RESPONSE) => void,
        routingKey: string,
    ) => void | Promise<void>,
    update: (
        message: UPDATE,
        send: (res: RESPONSE) => void,
        routingKey: string,
    ) => void | Promise<void>,
    any: (
        message: GENERIC,
        send: (res: RESPONSE) => void,
        routingKey: string,
    ) => void | Promise<void>,
    error: (
        error: Error,
    ) => void,
}

type A = { a: number };

type B = { b: null };

type C = A | B;

function test<Z, X extends Z, Y extends Z>(g: X, h: Y) {

}

export class RabbitNetworkHandler<GENERIC extends {msg_id: number, msg_intention: string, status: number},
    CREATE extends GENERIC,
    DELETE extends GENERIC,
    READ extends GENERIC,
    UPDATE extends GENERIC,
    RESPONSE extends {status: number, result: any}> extends AbstractBrokerHandler {

    /**
     * The event emitter supporting this network handler used to dispatch functions
     * @private
     */
    private _emitter = createNanoEvents<RabbitNetworkHandlerEvents<GENERIC, CREATE, DELETE, READ, UPDATE, RESPONSE>>();

    constructor(configuration: MessagingConfiguration, incomingValidator: DataValidator, outgoingValidator: DataValidator, connectionMethod?: ConnectFunction) {
        super(
            configuration,
            incomingValidator,
            outgoingValidator,
            connectionMethod,
        );
    }

    /**
     * Attaches an event listener to the underlying event emitter used by this network handler
     * @param event the event on which this listener should listen
     * @param callback the callback to be executed when the event is emitted
     */
    public on<E extends keyof RabbitNetworkHandlerEvents<GENERIC, CREATE, DELETE, READ, UPDATE, RESPONSE>>(
        event: E,
        callback: RabbitNetworkHandlerEvents<GENERIC, CREATE, DELETE, READ, UPDATE, RESPONSE>[E],
    ): Unsubscribe {
        return this._emitter.on(event, callback);
    }

    public once<E extends keyof RabbitNetworkHandlerEvents<GENERIC, CREATE, DELETE, READ, UPDATE, RESPONSE>>(
        event: E,
        callback: RabbitNetworkHandlerEvents<GENERIC, CREATE, DELETE, READ, UPDATE, RESPONSE>[E],
    ) {
        const unbind = this._emitter.on(event, (...args: any[]) => {
            unbind();

            // @ts-ignore
            void callback(...args);
        });

        return unbind;
    }

    /**
     * Handles a response from a a message handler validating the response and responding on the message broker to the
     * gateway. This should be used to generate callbacks for handlers.
     * @param User the initial User message that needs to be handled by this callback
     */
    private handleReply = (User: GENERIC) => (
        (response: RESPONSE): void => {
            __.info(`got a response to message ${User.msg_id} of status ${response.status}`);
            void super.send(User.msg_id, User.msg_intention, response);
        }
    );

    protected create(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void> {
        const cast = message as (CREATE);
        this._emitter.emit('create', cast, this.handleReply(cast), routingKey);
    }

    protected delete(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void> {
        const cast = message as (DELETE);
        this._emitter.emit('delete', cast, this.handleReply(cast), routingKey);
    }

    protected update(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void> {
        const cast = message as (UPDATE);
        this._emitter.emit('update', cast, this.handleReply(cast), routingKey);
    }

    protected read(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void> {
        const cast = message as (READ);
        this._emitter.emit('query', cast, this.handleReply(cast), routingKey);
    }

    protected other(message: Record<string, any>, routingKey: string): void | PromiseLike<void> | Promise<void> {
        const cast = message as (GENERIC);
        this._emitter.emit('any', cast, this.handleReply(cast), routingKey);
    }

    protected ready(): void | PromiseLike<void> | Promise<void> {
        this._emitter.emit('ready');
    }

    protected error(err?: any): void | PromiseLike<void> | Promise<void> {
        this._emitter.emit('error', err);
    }

}
