import {
    Collection, Filter,
    MongoError,
    ObjectId,
    OptionalId,
    OptionalUnlessRequiredId, UpdateFilter,
} from "mongodb";
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
    query: Filter<T>,
    id: string,
    details: Collection<T>,
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
    details: Collection<V>,
    duplicateHandler?: (e: MongoError) => Promise<void> | void,
    logger?: (id: string, action: string, additional?: Record<string, any>) => Promise<void> | void,
): Promise<string[]> {
    let result;

    try {
        result = await details.insertOne(mapper(entity) as OptionalUnlessRequiredId<V>);
    } catch (e: any) {
        if (e.code === 11000) {
            if (duplicateHandler) await duplicateHandler(e);

            throw new ClientFacingError('duplicate entity provided');
        }

        throw e;
    }


    if (result.insertedId === undefined) {
        throw new Error('failed to insert')
    }

    const id = (result.insertedId as ObjectId).toHexString();
    if (logger) await logger(id, 'inserted');

    return [id];
}

export async function genericUpdate<T extends { id: string }, K extends keyof T, C>(
    message: T,
    keys: K[],
    details: Collection<C>,
    additionalFilters: any = {},
    duplicateHandler?: (e: MongoError) => Promise<void> | void,
    keyManipulations?: Record<K, string>,
) {
    const filter: Filter<C> = {
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

    const changes: UpdateFilter<any> = {
        $set: set,
    }

    if (Object.keys(changes.$set ?? {}).length === 0) {
        throw new ClientFacingError('no operations provided');
    }

    let result;
    try {
        result = await details.updateOne(filter, changes as UpdateFilter<C>);
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

    if (result.modifiedCount !== 1) {
        throw new Error('failed to update');
    }

    return [message.id];
}
