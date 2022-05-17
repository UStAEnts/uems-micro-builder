import dayjs from "dayjs";
import * as util from "util";

/**
 * A set of logging functions for all levels which includes the origin
 */
type CompleteLogSet = {
    /**
     * Logs a message at the TRACE level
     * @param origin where this message originated in the system (within the module this is calling)
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    trace: (origin: string, message: string, ...data: any[]) => void,
    /**
     * Logs a message at the DEBUG level
     * @param origin where this message originated in the system (within the module this is calling)
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    debug: (origin: string, message: string, ...data: any[]) => void,
    /**
     * Logs a message at the INFO level
     * @param origin where this message originated in the system (within the module this is calling)
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    info: (origin: string, message: string, ...data: any[]) => void,
    /**
     * Logs a message at the WARN level
     * @param origin where this message originated in the system (within the module this is calling)
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    warn: (origin: string, message: string, ...data: any[]) => void,
    /**
     * Logs a message at the ERROR level
     * @param origin where this message originated in the system (within the module this is calling)
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    error: (origin: string, message: string, ...data: any[]) => void,
    /**
     * Logs a message at the FATAL level
     * @param origin where this message originated in the system (within the module this is calling)
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    fatal: (origin: string, message: string, ...data: any[]) => void,
};

/**
 * A set of logging functions for all levels which does not include the origin which should be identified elsewhere
 */
type OriginlessLogSet = {
    /**
     * Logs a message at the TRACE level, the origin of this log should be determined automatically elsewhere
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    trace: (message: string, ...data: any[]) => void,
    /**
     * Logs a message at the DEBUG level, the origin of this log should be determined automatically elsewhere
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    debug: (message: string, ...data: any[]) => void,
    /**
     * Logs a message at the INFO level, the origin of this log should be determined automatically elsewhere
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    info: (message: string, ...data: any[]) => void,
    /**
     * Logs a message at the WARN level, the origin of this log should be determined automatically elsewhere
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    warn: (message: string, ...data: any[]) => void,
    /**
     * Logs a message at the ERROR level, the origin of this log should be determined automatically elsewhere
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    error: (message: string, ...data: any[]) => void,
    /**
     * Logs a message at the FATAL level, the origin of this log should be determined automatically elsewhere
     * @param message the message to be logged to the output
     * @param data the optional data items which should be included in this log
     */
    fatal: (message: string, ...data: any[]) => void,
};

/**
 * The varying levels of log messages, ordered in severity
 */
type Level = 'TRACE' | 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' | 'FATAL';

/**
 * Maps log levels to a numerical values which allows ordering of log levels
 */
const LEVEL_MAPPING: Record<Level, Number> = {
    TRACE: 0,
    DEBUG: 1,
    INFO: 2,
    WARN: 3,
    ERROR: 4,
    FATAL: 5,
};

/**
 * A no operation function which returns the parameter passed to it
 * @param s the input and output value
 * @return the value of s
 */
const nop = (s: string) => s;
/**
 * Returns a function which will wrap a string in the ANSI escape code provided. This will be used for colour codes.
 * The string will be terminated with a reset foreground code.
 * @param code the ansi foreground colour code
 * @see https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
 */
const ansi = (code: number) => ((content: string) => `\u{1b}[${code}m${content}\u{1b}[39m`);

/**
 * Maps levels to a function which will wrap the text in the right colour for the level. If the process is being run
 * into a pipe (ie | cat) then it will disable all colours
 */
const LEVEL_COLOURS: Record<Level, (content: string) => string> = {
    // Trace = blue
    TRACE: process.stdout.isTTY ? ansi(34) : nop,
    // Debug = purple
    DEBUG: process.stdout.isTTY ? ansi(35) : nop,
    // Info = green
    INFO: process.stdout.isTTY ? ansi(32) : nop,
    // Warning = yellow
    WARN: process.stdout.isTTY ? ansi(33) : nop,
    // Error = red
    ERROR: process.stdout.isTTY ? ansi(31) : nop,
    // Fatal = bright red
    FATAL: process.stdout.isTTY ? ansi(91) : nop,
};

/**
 * Defines a transport which can be used to perform additional logging and processing
 */
export type Transport = (
    /**
     * The ID of the request trace in use. If null it indicates that this is a system level message, not associated with
     * a request
     */
    traceID: string | null,
    /**
     * The module from which this was logged. This will be provided from the configuration option {@link configuration}
     * which is setup via {@link configure}.
     */
    module: string,
    /**
     * Where this message originated within this module
     */
    origin: string,
    /**
     * The level at which this message was logged
     */
    level: Level,
    /**
     * The message to log
     */
    message: string,
    /**
     * Any associated data to be included in this request
     */
    data: any[]
) => void | Promise<void> | PromiseLike<void>;

/**
 * The type for the configuration of the log module
 */
type LogConfiguration = {
    /**
     * The name of this module which will be included in all configuration messages
     */
    module: string,
    /**
     * The level at which logs should be written to stdout
     */
    stdout: Level,
    /**
     * The set of transports which should be called when a log message is output
     */
    transports: Transport[],
    /**
     * The format to use for time in the output messages
     */
    timeFormat: string,
    /**
     * A formatter to be used to produce the log message to print on stdout
     * @param traceID The ID of the request trace in use. If null it indicates that this is a system level message, not
     * associated with a request
     * @param module The module from which this was logged. This will be provided from the configuration option
     * {@link configuration} which is setup via {@link configure}.
     * @param origin Where this message originated within this module
     * @param level The level at which this message was logged
     * @param message The message to log
     * @param data Any associated data to be included in this request
     */
    stdoutFormatter: (traceID: string | null, module: string, origin: string, level: Level, message: string, data: any[]) => string,
}


/**
 * Formats the given message using the default style
 *
 * @param traceID The ID of the request trace in use. If null it indicates that this is a system level message, not
 * associated with a request
 * @param module The module from which this was logged. This will be provided from the configuration option
 * {@link configuration} which is setup via {@link configure}.
 * @param origin Where this message originated within this module
 * @param level The level at which this message was logged
 * @param message The message to log
 * @param data Any associated data to be included in this request
 */
const stdoutFormatter = (traceID: string | null,
                         module: string,
                         origin: string,
                         level: Level,
                         message: string,
                         data: any[] | undefined) => {
    let output = `[${dayjs().format(configuration.timeFormat)}][${module}][${origin}]`;
    if (traceID) output += `[trace ${traceID}]`;
    output += `[${LEVEL_COLOURS[level](level)}] ${message}`;

    if (data !== undefined) {
        for (let i = 0; i < data.length; i++) {
            output += `\n\t[${i}]: `;
            output += util.inspect(data[i], false, null, process.stdout.isTTY).split('\n').map((e) => `\t${e}`).join('\n');
        }
    }

    return output;
}

/**
 * The currently active log configuration
 */
let configuration: LogConfiguration = {
    module: process.env.npm_package_name ?? 'unknown',
    stdout: 'INFO',
    transports: [],
    timeFormat: 'YYYY-MM-DD HH:mm:ss',
    stdoutFormatter,
};

/**
 * The internal log function which will dispatch the log to all the transports and log it to stdout if it meets the
 * minimum level required
 * @param traceID The ID of the request trace in use. If null it indicates that this is a system level message, not
 * associated with a request
 * @param module The module from which this was logged. This will be provided from the configuration option
 * {@link configuration} which is setup via {@link configure}.
 * @param origin Where this message originated within this module
 * @param level The level at which this message was logged
 * @param message The message to log
 * @param data Any associated data to be included in this request
 */
function logInternal(traceID: string | null,
                     module: string,
                     origin: string,
                     level: Level,
                     message: string,
                     data: any[] | undefined) {
    configuration.transports.forEach((transport) => transport(traceID, module, origin, level, message, data ?? []));
    if (LEVEL_MAPPING[level] >= LEVEL_MAPPING[configuration.stdout]) {
        console.log(configuration.stdoutFormatter(traceID, module, origin, level, message, data ?? []));
    }
}

/**
 * Wraps a transport and applies a level filter to it. This means the provided transport will only be called if the log
 * message is greater severity than the provided level
 * @param transport the transport which should be wrapped
 * @param level the level at which this transport should be called
 */
export function filter(transport: Transport, level: Level): Transport {
    return (traceID, module, origin, level1, message, data) => {
        if (LEVEL_MAPPING[level1] >= LEVEL_MAPPING[level]) transport(traceID, module, origin, level1, message, data);
    }
}

/**
 * Configures the active log entity, either overwriting transports or merging them
 * @param config the configuration which should be applied
 * @param method if merge is given, then any transports will be added to the set. If replace is provided then it will
 * replace the existing transports
 */
export function configure(config: Partial<LogConfiguration>, method: 'replace' | 'merge' = 'merge') {
    // Merging means just mixing in the transports rather than replacing the list
    if (method === 'merge') {
        if (Object.prototype.hasOwnProperty.call(config, 'transports') && config.transports !== undefined) {
            configuration.transports.push(...config.transports);
            delete config.transports;
        }
    }

    configuration = Object.assign(configuration, config);
}

type TraceToComplete = (traceID: string) => CompleteLogSet;
type TraceToOriginless = (traceID: string) => OriginlessLogSet;

type LogType = TraceToComplete
    & {
    system: CompleteLogSet,
    auto: TraceToOriginless
        & {
        system: OriginlessLogSet,
        manual: TraceToComplete
            & {
            system: CompleteLogSet,
        },
    },
    derive: (origin: string) => TraceToOriginless
        & {
        system: OriginlessLogSet,
    },
};

/**
 * Retrieves the origin of the call 'depth' levels higher. This will generate an error and extract the stack trace
 * pulling the record 'depth' entries down and removing the 'at'. If for any reason the stack is not long enough,
 * undefined or any other issues, it will just return 'unknown'
 * @param depth how many layers down in the call stack is should go
 */
function findOrigin(depth: number): string {
    const stack = new Error().stack;
    if (stack === undefined) return 'unknown';

    const lines = stack.split('\n').map((e) => e.trim()).slice(1);
    if (depth >= lines.length) return 'unknown';

    const entry = lines[depth];
    const components = entry.split(' ');
    if (components.length < 2) return 'unknown';

    return components.slice(1).join(' ');
}

const basicLog: ((id: string | null) => CompleteLogSet) = (traceID) => ({
    fatal: (origin, message, ...data) => logInternal(traceID, configuration.module, origin, 'FATAL', message, data),
    error: (origin, message, ...data) => logInternal(traceID, configuration.module, origin, 'ERROR', message, data),
    warn: (origin, message, ...data) => logInternal(traceID, configuration.module, origin, 'WARN', message, data),
    info: (origin, message, ...data) => logInternal(traceID, configuration.module, origin, 'INFO', message, data),
    debug: (origin, message, ...data) => logInternal(traceID, configuration.module, origin, 'DEBUG', message, data),
    trace: (origin, message, ...data) => logInternal(traceID, configuration.module, origin, 'TRACE', message, data),
});

const originless: ((n: number) => (t: string | null) => OriginlessLogSet) = (depth: number) => ((traceID: string | null) => ({
    fatal: (message, ...data) => logInternal(traceID, configuration.module, findOrigin(depth), 'FATAL', message, data),
    error: (message, ...data) => logInternal(traceID, configuration.module, findOrigin(depth), 'ERROR', message, data),
    warn: (message, ...data) => logInternal(traceID, configuration.module, findOrigin(depth), 'WARN', message, data),
    info: (message, ...data) => logInternal(traceID, configuration.module, findOrigin(depth), 'INFO', message, data),
    debug: (message, ...data) => logInternal(traceID, configuration.module, findOrigin(depth), 'DEBUG', message, data),
    trace: (message, ...data) => logInternal(traceID, configuration.module, findOrigin(depth), 'TRACE', message, data),
}))

const fixed: ((n: string) => (t: string | null) => OriginlessLogSet) = (fixed: string) => ((traceID: string | null) => ({
    fatal: (message, ...data) => logInternal(traceID, configuration.module, fixed, 'FATAL', message, data),
    error: (message, ...data) => logInternal(traceID, configuration.module, fixed, 'ERROR', message, data),
    warn: (message, ...data) => logInternal(traceID, configuration.module, fixed, 'WARN', message, data),
    info: (message, ...data) => logInternal(traceID, configuration.module, fixed, 'INFO', message, data),
    debug: (message, ...data) => logInternal(traceID, configuration.module, fixed, 'DEBUG', message, data),
    trace: (message, ...data) => logInternal(traceID, configuration.module, fixed, 'TRACE', message, data),
}))

const log: LogType = Object.assign(basicLog, {
    system: basicLog(null),
    auto: Object.assign(
        originless(2),
        {
            manual: Object.assign(
                basicLog,
                {
                    system: basicLog(null)
                },
            ),
            system: originless(2)(null),
        }
    ),
    derive: (origin: string) => Object.assign(
        fixed(origin),
        {
            system: fixed(origin)(null)
        }
    )
});

export default log;