import { DatabaseEvents, GenericDatabase } from "./GenericDatabase";
import { createNanoEvents, Unsubscribe } from "nanoevents";
import { Collection, Db, MongoClient, MongoClientOptions, ObjectId } from "mongodb";
import winston from "winston";
import { tryApplyTrait } from "../healthcheck/Healthcheck";

const __ = winston.child({ label: __filename });

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

    private _configuration?: MongoDBConfiguration;

    /**
     * If this class currently has an good connection to the database
     * @private
     */
    private _connected = false;

    public constructor(_configuration: MongoDBConfiguration);
    public constructor(database: Db, collections: MongoDBConfiguration['collections']);
    // TODO: remove this shim?
    public constructor(_configurationOrDB: MongoDBConfiguration | Db, collections?: MongoDBConfiguration['collections']);
    constructor(
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

            __.debug('creating database using a new database connection');
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
                this._database = client.db(conf.database);

                this._database.on('close', () => tryApplyTrait('db.status', 'disconnected'));
                this._database.on('disconnect', () => tryApplyTrait('db.status', 'disconnected'));
                this._database.on('reconnect', () => tryApplyTrait('db.status', 'connected'));
                this._database.on('connect', () => tryApplyTrait('db.status', 'connected'));
                tryApplyTrait('db.status', 'connected');

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
            __.debug('creating database using an existing database connection', { collections });
            // If collections is defined then we need the second constructor where the first param is a db
            this._database = db;

            this._database.on('close', () => tryApplyTrait('db.status', 'disconnected'));
            this._database.on('disconnect', () => tryApplyTrait('db.status', 'disconnected'));
            this._database.on('reconnect', () => tryApplyTrait('db.status', 'connected'));
            this._database.on('connect', () => tryApplyTrait('db.status', 'connected'));
            tryApplyTrait('db.status', 'connected');

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

    protected abstract createImpl(create: CREATE, details: Collection, changelog: Collection): Promise<string[]>;

    protected abstract deleteImpl(create: DELETE, details: Collection, changelog: Collection): Promise<string[]>;

    protected abstract updateImpl(create: UPDATE, details: Collection, changelog: Collection): Promise<string[]>;

    protected abstract queryImpl(create: READ, details: Collection, changelog: Collection): Promise<REPRESENTATION[]>;


    public create = async (create: CREATE): Promise<string[]> => {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.createImpl(create, this._details, this._changelog);
    }

    async delete(del: DELETE): Promise<string[]> {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.deleteImpl(del, this._details, this._changelog);
    }

    async query(query: READ): Promise<REPRESENTATION[]> {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.queryImpl(query, this._details, this._changelog);
    }

    async update(update: UPDATE): Promise<string[]> {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        return this.updateImpl(update, this._details, this._changelog);
    }

    protected async log(id: string, action: string, additional: Record<string, any> = {}) {
        if (!this._changelog) return;

        try {
            await this._changelog.insertOne({
                ...additional,
                id,
                action,
                timestamp: Date.now(),
            });
        } catch (e) {
            console.warn('Failed to save changelog');
        }
    }

    protected defaultDelete = async (del: { id: string }): Promise<string[]> => {
        if (!this._connected || !this._database || !this._details || !this._changelog)
            throw new Error('create called before database was ready');

        const { id } = del;
        if (!ObjectId.isValid(id)) {
            throw new Error('invalid object ID');
        }

        const objectID = ObjectId.createFromHexString(id);
        const query = {
            _id: objectID,
        };

        __.debug('executing delete query', {
            query,
        });

        // TODO: validating the incoming ID
        const result = await this._details
            .deleteOne(query);

        if (result.result.ok !== 1 || result.deletedCount !== 1) {
            throw new Error('failed to delete');
        }

        await this.log(id, 'deleted');

        return [id];
    }


}
