import { Collection, FilterQuery, MongoError, ObjectId, UpdateQuery } from "mongodb";
import { ClientFacingError } from "../errors/ClientFacingError";

export const stripUndefined = <T>(data: T): T => Object.fromEntries(Object.entries(data).filter(([, value]) => value !== undefined)) as unknown as T;

export function genericEntityConversion<T,
    V,
    K_IN extends Extract<keyof T, string>,
    K_OUT extends Extract<keyof V, string>>(entity: T, mappings: Record<K_IN, K_OUT>, objectID?: string): V {

    // @ts-ignore
    const output: Record<K_OUT, any> = {};

    for (const [inKey, outKey] of Object.entries(mappings) as [K_IN, K_OUT][]) {
        if (objectID !== undefined && objectID === inKey && entity[inKey] !== undefined) {
            output[outKey] = (entity[inKey] as unknown as ObjectId).toHexString() as any;
        } else {
            if (entity[inKey] !== undefined) output[outKey] = entity[inKey];
        }
    }

    return output as V;
}

export async function genericDelete<T>(
    query: FilterQuery<T>,
    id: string,
    details: Collection,
    logger?: (id: string, action: string, additional?: Record<string, any>) => Promise<void> | void,
): Promise<string[]> {
    let result;

    try {
        result = await details.deleteOne(query);
    } catch (e) {
        throw e;
    }

    if (result.deletedCount !== 1) {
        throw new ClientFacingError('invalid entity ID');
    }

    if (logger) await logger(id, 'inserted');

    return [id];
}

export async function genericCreate<T, V>(
    entity: T,
    mapper: (input: T) => V,
    details: Collection,
    duplicateHandler?: (e: MongoError) => Promise<void> | void,
    logger?: (id: string, action: string, additional?: Record<string, any>) => Promise<void> | void,
): Promise<string[]> {
    let result;

    try {
        result = await details.insertOne(mapper(entity));
    } catch (e: any) {
        if (e.code === 11000) {
            if (duplicateHandler) await duplicateHandler(e);

            throw new ClientFacingError('duplicate entity provided');
        }

        throw e;
    }

    if (result.insertedCount !== 1 || result.insertedId === undefined) {
        throw new Error('failed to insert')
    }

    const id = (result.insertedId as ObjectId).toHexString();
    if (logger) await logger(id, 'inserted');

    return [id];
}

export async function genericUpdate<T extends { id: string }, K extends keyof T>(
    message: T,
    keys: K[],
    details: Collection,
    additionalFilters: any = {},
    duplicateHandler?: (e: MongoError) => Promise<void> | void,
    keyManipulations?: Record<K, string>,
) {
    const filter: FilterQuery<any> = {
        _id: new ObjectId(message.id),
        ...additionalFilters,
    };

    const set: Record<string, any> = {};

    for (const key of keys) {
        if (message[key] !== undefined) {
            if (keyManipulations && keyManipulations.hasOwnProperty(key)) {
                set[keyManipulations[key]] = message[key];
            } else {
                set[key as string] = message[key];
            }
        }
    }

    const changes: UpdateQuery<any> = {
        $set: set,
    }

    if (Object.keys(changes.$set ?? {}).length === 0) {
        throw new ClientFacingError('no operations provided');
    }

    let result;
    try {
        result = await details.updateOne(filter, changes);
    } catch (e:any) {
        if (e.code === 11000) {
            if (duplicateHandler) await duplicateHandler(e);

            throw new ClientFacingError('duplicate entity provided');
        }

        throw e;
    }

    if (result.matchedCount === 0) {
        throw new ClientFacingError('invalid entity ID');
    }

    if (result.result.ok !== 1) {
        throw new Error('failed to update');
    }

    return [message.id];
}
