import winston from "winston";

winston.add(new winston.transports.Console());

import { Db, MongoClient } from "mongodb";
import MongoUnit from "mongo-unit";

export function haveNoAdditionalKeys(object: any, allowed: string[]) {
    for (const key of Object.keys(object)) {
        expect(allowed).toContain(key);
    }
}

export async function defaultBeforeAll(): Promise<{ client: MongoClient, db: Db }> {
    // Setup the in memory mongo db database
    await MongoUnit.start();

    // Create the database connection and connect to the one we just created in memory
    const client = new MongoClient(MongoUnit.getUrl(), {
        useUnifiedTopology: true,
    });
    await client.connect();

    // Then create a user database around this
    const db = client.db('testing');

    return { client, db };
}

export async function defaultAfterAll(client: MongoClient, db: Db) {
    // Shutdown our connection to the database
    await client.close();

    // Then stop the in memory database
    await MongoUnit.stop();
}

export async function defaultBeforeEach(initialData: any[], client: MongoClient, db: Db, collection: string = 'details') {
    if (initialData.length > 0) await db.collection(collection).insertMany(initialData);
}

export async function defaultAfterEach(client: MongoClient, db: Db, collections: string = 'details') {
    await db.collection(collections).deleteMany({});
    await MongoUnit.drop();
}
