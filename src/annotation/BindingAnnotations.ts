import { AbstractBrokerHandler } from "../messaging/AbstractBrokerHandler";
import { RabbitNetworkHandler } from "../messaging/GenericRabbitNetworkHandler";
import log from "../logging/Log";

const _ = log.auto.system;

/**
 * The currently defined handler instance which will be used for any calls to bind() and is defined by binder()
 */
let brokerHandlerInstance: RabbitNetworkHandler<any, any, any, any, any, any>;

/**
 * The routing key entry to represent any routing key possible
 */
export const ANY = Symbol.for('routing-key-any');

/**
 * Define the global binding instance. This will not move any previous bindings but and calls to bind() after this will
 * apply to this handler instead
 * @param handler the new handler to use globally.
 */
export function binder(handler: RabbitNetworkHandler<any, any, any, any, any, any>) {
	brokerHandlerInstance = handler;
}

/**
 * Bind to the create event from the broker
 * @param instance the instance to which this should bind
 * @param action 'create'
 * @param routingKey the routing key which needs to be matched
 * @param f the function which should be executed on message
 */
export function bindTo<GENERIC extends { msg_id: number, msg_intention: string, status: number },
	CREATE extends GENERIC,
	RESPONSE extends { status: number, result: any },
	HANDLER extends RabbitNetworkHandler<GENERIC, CREATE, any, any, any, RESPONSE>>(
	instance: HANDLER,
	action: 'create',
	routingKey: string | RegExp | typeof ANY,
	f: (evt: CREATE, repl: (r: RESPONSE) => void, routing?: string) => void,
): void;

/**
 * Bind to the delete event from the broker
 * @param instance the instance to which this should bind
 * @param action 'delete'
 * @param routingKey the routing key which needs to be matched
 * @param f the function which should be executed on message
 */
export function bindTo<GENERIC extends { msg_id: number, msg_intention: string, status: number },
	DELETE extends GENERIC,
	RESPONSE extends { status: number, result: any },
	HANDLER extends RabbitNetworkHandler<GENERIC, any, DELETE, any, any, RESPONSE>>(
	instance: HANDLER,
	action: 'delete',
	routingKey: string | RegExp | typeof ANY,
	f: (evt: DELETE, repl: (r: RESPONSE) => void, routing?: string) => void,
): void;

/**
 * Bind to the query event from the broker
 * @param instance the instance to which this should bind
 * @param action 'query'
 * @param routingKey the routing key which needs to be matched
 * @param f the function which should be executed on message
 */
export function bindTo<GENERIC extends { msg_id: number, msg_intention: string, status: number },
	QUERY extends GENERIC,
	RESPONSE extends { status: number, result: any },
	HANDLER extends RabbitNetworkHandler<GENERIC, any, any, QUERY, any, RESPONSE>>(
	instance: HANDLER,
	action: 'query',
	routingKey: string | RegExp | typeof ANY,
	f: (evt: QUERY, repl: (r: RESPONSE) => void, routing?: string) => void,
): void;

/**
 * Bind to the update event from the broker
 * @param instance the instance to which this should bind
 * @param action 'update'
 * @param routingKey the routing key which needs to be matched
 * @param f the function which should be executed on message
 */
export function bindTo<GENERIC extends { msg_id: number, msg_intention: string, status: number },
	UPDATE extends GENERIC,
	RESPONSE extends { status: number, result: any },
	HANDLER extends RabbitNetworkHandler<GENERIC, any, any, any, UPDATE, RESPONSE>>(
	instance: HANDLER,
	action: 'update',
	routingKey: string | RegExp | typeof ANY,
	f: (evt: UPDATE, repl: (r: RESPONSE) => void, routing?: string) => void,
): void;

/**
 * Bind to an event from the broker
 *
 * @param instance the instance to which this should bind
 * @param action the action on which this should response
 * @param routingKey the routing key which needs to be matched
 * @param f the function which should be executed on message
 */
export function bindTo<GENERIC extends { msg_id: number, msg_intention: string, status: number },
	CREATE extends GENERIC,
	DELETE extends GENERIC,
	READ extends GENERIC,
	UPDATE extends GENERIC,
	RESPONSE extends { status: number, result: any },
	HANDLER extends RabbitNetworkHandler<GENERIC, CREATE, DELETE, READ, UPDATE, RESPONSE>>(
	instance: HANDLER,
	action: 'create' | 'delete' | 'query' | 'update' | 'any',
	routingKey: string | RegExp | typeof ANY,
	f: (evt: CREATE | DELETE | READ | UPDATE, repl: (r: RESPONSE) => void, routing?: string) => void,
) {
	_.debug(`on receive ${ action } with routing key ${ String(routingKey) } execute ${ f.name ?? '<anonymous>' }`);
	instance.on(action, <T extends CREATE | DELETE | READ | UPDATE,>(evt: T, send: any, routing: string) => {
		if (typeof (routingKey) === 'string' && routing !== routingKey) return;
		if (typeof (routingKey) === 'object' && !routingKey.test(routing)) return;

		if (action === 'create' && evt.msg_intention !== 'CREATE') throw new Error(`Invalid message received: expected CREATE got ${evt.msg_intention}`);
		if (action === 'delete' && evt.msg_intention !== 'DELETE') throw new Error(`Invalid message received: expected DELETE got ${evt.msg_intention}`);
		if (action === 'query' && evt.msg_intention !== 'READ') throw new Error(`Invalid message received: expected READ got ${evt.msg_intention}`);
		if (action === 'update' && evt.msg_intention !== 'UPDATE') throw new Error(`Invalid message received: expected UPDATE got ${evt.msg_intention}`);

		f(evt, send, routing);
	});
}


/**
 * Bind to an event from the globally defined broker via binder(). If not defined this will throw an exception
 *
 * @param action the action on which this should response
 * @param routingKey the routing key which needs to be matched
 * @param f the function which should be executed on message
 */
export function bind<T, R>(
	action: 'create' | 'delete' | 'query' | 'update' | 'any',
	routingKey: string | RegExp | typeof ANY,
	f: (evt: T, repl: (r: R) => void, routing?: string) => void,
) {
	if (!brokerHandlerInstance) throw new Error("Binder has not been configured - have you called binder(...) yet?");

	_.debug(`on receive ${ action } with routing key ${ String(routingKey) } execute ${ f.name ?? '<anonymous>' }`);
	brokerHandlerInstance.on(action, (evt: any, send: any, routing: string) => {
		if (typeof (routingKey) === 'string' && routing !== routingKey) return;
		if (typeof (routingKey) === 'object' && !routingKey.test(routing)) return;
		f(evt, send, routing);
	});
}

/**
 * Returns the currently defined instance of the global broker.
 */
export default function getInstance() {
	return brokerHandlerInstance;
}