import {IncomingMessage, Server, ServerResponse} from "http";
import {ObjectUtilities} from "@uems/uemscommlib";
import has = ObjectUtilities.has;
import log from "../logging/Log";

type StatusOption = 'healthy' | 'unhealthy-serving' | 'unhealthy';

class Healthcheck {

    /**
     * The default port that this server will run on if the port is not defined
     * @private
     */
    private readonly DEFAULT_PORT = process.env.UEMS_HEALTHCHECK ? Number(process.env.UEMS_HEALTHCHECK) : 7777;

    /**
     * The webserver that this healthcheck endpoint is running on. This will only be defined if the server has been
     * launched. This can be used to close the server
     * @private
     */
    private _server: Server;
    /**
     * The port on which the server should be running and will be used when the server launches
     * @private
     */
    private _port: number;
    /**
     * Holds the currently defined properties of the system. These will be reported against the status endpoint
     * @private
     */
    private _activeTraits: Record<string, any> = {};
    /**
     * All properties that should eventually be or could be defined by this system. This ensures that the response from
     * the server is always complete, even if properties are undefined.
     * @private
     */
    private _declaredTraits: string[];

    /**
     * A validator to be run against the defined traits. This should return whether the
     * @private
     */
    private readonly _validator: (traits: Healthcheck['_activeTraits']) => StatusOption;

    /**
     * If the state of the validator shoud be logged to stdout - this can cause a busy output so should be used for
     * debugging more than anything
     * @private
     */
    private readonly _log: boolean;

    /**
     * Creates a new healthcheck server but doesn't make it listen.
     * @param declaredTraits the traits that should be included by default on the responses. If not specified it will
     * set them equal to _undefined in the response of the request
     * @param validator a validator to be applied to the traits, should return the current state of the server based
     * on the set of current traits
     * @param port the port the server should be started on. Defaults to {@link DEFAULT_PORT} if not specified
     * @param log log every update to the healthcheck system
     */
    constructor(declaredTraits: string[], validator: Healthcheck['_validator'], port?: number, log?: boolean) {
        this._port = port ?? this.DEFAULT_PORT;
        this._declaredTraits = declaredTraits;
        this._validator = validator;
        this._server = new Server(this.handle.bind(this));
        this._log = log ?? false;
    }

    /**
     * Launches the server on the specified port or the default
     */
    public async launch() {
        if (this._server.listening) throw new Error('healthcheck is already listening');
        return new Promise<void>((resolve) => {
            this._server.listen(this._port, () => {
                log.auto.system.info(`\u{1F5A4} healthcheck enabled on :${this._port}`)
                resolve();
            });
        });
    }

    /**
     * Closes the server if its currently running
     */
    public async close() {
        if (!this._server.listening) throw new Error('healthcheck is not listening');
        return new Promise<void>((resolve, reject) => {
            this._server.close((err) => {
                if (err) reject();
                else resolve();
            });
        });
    }

    /**
     * Defines a trait which will included in the response
     * @param key the key this value will be associated with
     * @param value the value of this trait
     */
    public trait(key: string, value: any) {
        if (this._log) log.system.trace('mb:Healthcheck:trait', `trait [${key}] has updated status to [${value}]`);
        this._activeTraits[key] = value;
    }

    /**
     * Builds the response of the current set of traits. Setting any declared traits that are not in the request to
     * _undefined
     * @private
     */
    private buildResponse() {
        const clone = {...this._activeTraits};
        this._declaredTraits
            .filter((e) => !clone.hasOwnProperty(e))
            .forEach((key) => clone[key] = "_undefined");
        return clone;
    }

    /**
     * The handle function of the server. Will only response to /healthcheck. If the validator throws an error this will
     * be marked as unhealthy. If the status is healthy the server has a status code of 200, otherwise it gives 500
     * for easy detection. The full set of traits is provided in the response along with the status string.
     * @param req the request received by the http server
     * @param res the response to which the server should write
     * @private
     */
    private handle(req: IncomingMessage, res: ServerResponse) {
        if (req.url !== '/healthcheck') {
            res.statusCode = 404;
            res.end();
            return;
        }

        let status;

        try {
            status = this._validator(this._activeTraits);
        } catch (e) {
            if (this._log) log.system.trace('mb:Healthcheck:handle', 'Validator failed: ', e);
            status = 'unhealthy';
        }

        if (status === 'healthy') res.statusCode = 200;
        else res.statusCode = 500;

        res.setHeader('Content-Type', 'application/json');
        res.write(JSON.stringify({
            ...this.buildResponse(),
            status,
        }));
        res.end();
    }

}

export type HealthcheckServer = Healthcheck;

/**
 * Only one healthcheck should be available on the server at any times
 */
let activeHealthcheck: Healthcheck | undefined = undefined;

/**
 * Launches the healthcheck server and returns the newly defined instance. It also writes it to the active healthcheck
 * variable so it will be shared between the server.
 * @param traits the set of traits to define on the health check
 * @param validator the validator which should be applied to the response before sending to determine the server status
 * @param port the port on which the server should run
 * @param log log every update to the healthcheck system
 */
export async function launchCheck(traits: string[], validator: Healthcheck['_validator'], port?: number, log?: boolean) {
    if (activeHealthcheck !== undefined) throw new Error('multiple healthchecks cannot be created');
    const value = new Healthcheck(traits, validator, port, log);
    await value.launch();

    activeHealthcheck = value;
    return value;
}

export function health(
    traits: string[],
    present: string[],
    requiredValues: Record<string, any>,
    evaluate: Record<string, ((value: any) => StatusOption)>,
    thresholds: { condition: string[], over: string[], threshold: number, result: 'unhealthy' | 'unhealthy-serving' }[],
    port?: number,
    trace?: boolean,
) {
    const reduce = (a: StatusOption, b: StatusOption): StatusOption => {
        if (a === 'unhealthy' || b === 'unhealthy') return 'unhealthy';
        if (a === 'unhealthy-serving' || b === 'unhealthy-serving') return 'unhealthy-serving';
        return 'healthy';
    }

    const _log = trace
        ? log.system.trace
        : () => undefined;

    return {
        traits,
        validator: (status: Record<string, any>) => {
            const p: StatusOption = present.length === 0 ? 'healthy' : present.map((e) => {
                const contains = has(status, e);
                if (!contains) _log('mb:Healthcheck:health', `unhealthy: value [${e}] is missing`);
                return contains;
            })
                .reduce((a, b) => a && b, true) ? 'healthy' : 'unhealthy';

            const required: StatusOption = Object.entries(requiredValues).map(([key, value]): 'unhealthy' | 'healthy' => {
                if (!has(status, key)) {
                    _log('mb:Healthcheck:health', `unhealthy: value [${key}] is missing`);
                    return 'unhealthy';
                }
                if (status[key] !== value) {
                    _log('mb:Healthcheck:health', `unhealthy: [${key}] is currently [${status[key]}] but is required to be [${value}]`);
                    return 'unhealthy';
                }

                return 'healthy';
            }).reduce((a, b): StatusOption => {
                return reduce(a as any, b as any);
            }, 'healthy' as StatusOption) as StatusOption;

            const evaluated: StatusOption = Object.entries(evaluate).map(([key, value]) => {
                const result = value(status[key]);
                if (result === 'unhealthy-serving' || result === 'unhealthy') {
                    _log('mb:Healthcheck:health', `${result}: [${key}] evaluated`);
                }
                return result;
            })
                .reduce((a, b) => reduce(a, b), 'healthy');

            const ts: StatusOption = thresholds
                .map(({condition, over, threshold, result}) => {
                    const top = condition.map((e) => status[e]).reduce((a, b) => a + b, 0);
                    const bottom = (over.map((e) => status[e]).reduce((a, b) => a + b, 0) + 0.0);

                    if ((top / bottom) > threshold) {
                        _log('mb:Healthcheck:health', `${result}: ${top} / ${bottom} > ${threshold}`);
                        return result;
                    }

                    return 'healthy';
                }).reduce((a, b) => reduce(a, b), 'healthy');

            _log('mb:Healthcheck:health', `result`, p, required, evaluated, ts);
            return reduce(p, reduce(required, reduce(evaluated, ts)));
        },
        port,
        log: trace,
    };
}

export async function healthcheck(config: { traits: string[], validator: Healthcheck['_validator'], port?: number, log?: boolean }) {
    return launchCheck(config.traits, config.validator, config.port, config.log);
}

/**
 * Returns the currently defined healthcheck server or throws an exception if it has not been defined
 * @returns the currently active healthceck server on which the traits should be defined
 */
export default function getHealthcheck() {
    if (activeHealthcheck === undefined) throw new Error('healthcheck has not been defined');
    return activeHealthcheck;
}

/**
 * Attempts to apply a trait to the active healthcheck server. If it is undefined it will do nothing
 * @param key the key of the trait to define
 * @param value the value of this trait
 */
export function tryApplyTrait(key: string, value: any) {
    if (activeHealthcheck !== undefined) activeHealthcheck.trait(key, value);
}

/**
 * Clears the currently defined healthcheck. This is primarily provided for testing purposes but can be
 * used if the server needs to redefine the healthcheck at a certain point. However, if you find yourself needing
 * to use this function you may need to reconsider your design choices.
 */
export function _clearHealthcheck() {
    activeHealthcheck = undefined;
}
