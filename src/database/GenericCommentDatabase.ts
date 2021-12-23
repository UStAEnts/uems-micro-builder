import { GenericMongoDatabase, MongoDBConfiguration } from "./GenericMongoDatabase";
import { CommentMessage, CommentResponse } from "@uems/uemscommlib";
import { Collection, Db, FilterQuery, ObjectId, UpdateQuery } from "mongodb";
import ReadCommentMessage = CommentMessage.ReadCommentMessage;
import DeleteCommentMessage = CommentMessage.DeleteCommentMessage;
import InternalComment = CommentResponse.InternalComment;
import ShallowInternalComment = CommentResponse.ShallowInternalComment;
import CreateCommentMessage = CommentMessage.CreateCommentMessage;
import UpdateCommentMessage = CommentMessage.UpdateCommentMessage;
import { genericDelete } from "../utility/GenericDatabaseFunctions";

/**
 * The schema of the objects when they are held in the database
 */
export type DatabaseComment = {
    /**
     * The object ID of this comment, generated by mongodb
     */
    _id: string | ObjectId,
    /**
     * The type of asset that this comment is attached to. This is used to distinguish multiple assets being held in the
     * database
     */
    assetType: string,
    /**
     * The unique identifier of the asset
     */
    assetID: string,
    /**
     * The user ID of the user who posted this comment
     */
    poster: string,
    /**
     * The timestamp at which this comment was posted
     */
    posted: number,
    /**
     * The optional category of this comment
     */
    topic: string | null,
    /**
     * If this comment requires attention from another user
     */
    requiredAttention: boolean,
    /**
     * If this comment has been attended to and if so who attended to the comment
     */
    attendedBy: string | null;
    /**
     * When someone attended to the comment and resolved it
     */
    attendedAt: number | null,
    /**
     * The body of the comment
     */
    body: string,
}

/**
 * A clone of the {@link DatabaseComment} type but without the id entity made optional and forced to a string type
 */
export type CreateDatabaseComment = Omit<DatabaseComment, '_id'> & { _id?: string };

/**
 * Converts a database comment into a shallow internal comment. This ensures that no additional properties that are
 * accidentally included in the database will be exposed to the client.
 * @param db the entity from the database
 */
const transmute = (db: DatabaseComment): ShallowInternalComment => {
    return {
        assetType: db.assetType,
        id: typeof (db._id) === 'string' ? db._id : db._id.toHexString(),
        assetID: db.assetID,
        attendedBy: db.attendedBy ?? undefined,
        attendedDate: db.attendedAt ?? undefined,
        poster: db.poster,
        topic: db.topic ?? undefined,
        requiresAttention: db.requiredAttention,
        posted: db.posted,
        body: db.body,
    };
}

/**
 * A generic implementation of a comment database build on the back of the generic mongo DB database. This provides full
 * CRUD support for comments with enforcement of asset types to ensure that the comments interacted with through this
 * database do not interfere with each other
 */
export class GenericCommentDatabase extends GenericMongoDatabase<ReadCommentMessage, CreateCommentMessage, DeleteCommentMessage, UpdateCommentMessage, ShallowInternalComment> {

    /**
     * The set of assets which can be read/updated/created/deleted via this database instance
     * @private
     */
    private _assetType: string[];

    /**
     * Creates a new database instance and will assert that the index exists on the database. This will be performed
     * straight away if the database is already configured or once it is ready.
     * @param assetType the set of assets which can be manipulated via this database
     * @param configuration the configuration which should be used to construct the new database connection
     */
    constructor(assetType: string[], configuration: MongoDBConfiguration);
    /**
     * Reuses a database instance using the provided collections. The index on body will be created to assert that it
     * exists as soon as the collection is prepared
     * @param assetType the set of assets which can be manipulated via the database
     * @param db the database on which this comment table should sit
     * @param collections the set of collections through which this comment database should interact
     */
    constructor(assetType: string[], db: Db, collections: MongoDBConfiguration['collections']);
    constructor(assetType: string[], _configurationOrDB: MongoDBConfiguration | Db, collections?: MongoDBConfiguration['collections']) {
        super(_configurationOrDB, collections);
        this._assetType = assetType;


        const register = (details: Collection) => {
            void details.createIndex({ body: 'text' });
        };

        if (this._details) {
            register(this._details);
        } else {
            this.once('ready', () => {
                if (!this._details) throw new Error('Details db was not initialised on ready');
                register(this._details);
            });
        }
    }

    /**
     * Creates a new comment in the database. This will assert that the asset type given in the create message is within
     * the acceptable list
     * @param create the message which contains the properties used to create the comment
     * @param details the details collection into which the comment should be inserted
     * @param changelog the collection into which the changelog should be managed
     * @protected
     */
    protected async createImpl(create: CreateCommentMessage, details: Collection, changelog: Collection): Promise<string[]> {
        if (!this._assetType.includes(create.assetType)) {
            throw new Error('Invalid asset type');
        }

        const entity: CreateDatabaseComment = {
            requiredAttention: create.requiresAttention ?? false,
            topic: create.topic ?? null,
            poster: create.posterID,
            attendedAt: null,
            attendedBy: null,
            assetID: create.assetID,
            assetType: create.assetType,
            posted: Date.now(),
            body: create.body,
        }

        const result = await details.insertOne(entity);

        if (result.insertedCount !== 1 || result.insertedId === undefined) {
            throw new Error('failed to insert')
        }

        return [(result.insertedId as ObjectId).toHexString()];
    }

    /**
     * Deletes the comment given by the delete comment. This will also impose an asset type filter to ensrure that no
     * additional comments can be removed via this entity
     * @param remove the details of the comment to remove
     * @param details the details collection into which the comment should be removed
     * @param changelog the collection into which the changelog should be managed
     * @protected
     */
    protected deleteImpl(remove: DeleteCommentMessage, details: Collection, changelog: Collection): Promise<string[]> {
        if (!ObjectId.isValid(remove.id)) {
            throw new Error('invalid object id');
        }

        return genericDelete<DatabaseComment>({
            _id: new ObjectId(remove.id),
            assetType: {
                $in: this._assetType,
            },
        }, remove.id, details, this.log.bind(this));
    }

    /**
     * Returns all database entires that match the given query. If an asset type is provided it will be checked to
     * ensure its within the valid list, otherwise a generic asset type filter will be applied with all the valid types
     * @param query the query to apply to the database
     * @param details the details collection in which the database entries are held
     * @param changelog the changelog into which any changes should be recorded
     * @protected
     */
    protected async queryImpl(query: ReadCommentMessage, details: Collection, changelog: Collection): Promise<ShallowInternalComment[]> {
        if (query.assetType && !this._assetType.includes(query.assetType)) {
            throw new Error('Invalid asset type');
        }

        const exec: FilterQuery<DatabaseComment> = {};

        if (query.body) {
            exec.$text = {
                $search: query.body,
            }
        }

        if (query.requiresAttention) exec.requiredAttention = true;
        if (query.topic) exec.topic = query.topic;
        if (query.assetID) exec.assetID = query.assetID;
        if (query.posterID) exec.poster = query.posterID;
        if (query.attended) exec.attendedBy = { $exists: true, };
        if (query.posted) exec.posted = query.posted;
        if (query.id) exec._id = new ObjectId(query.id);
        if (query.assetType) exec.assetType = query.assetType;
        else exec.assetType = {
            $in: this._assetType,
        };

        const filter: DatabaseComment[] = await details.find(exec).toArray();
        const result: ShallowInternalComment[] = filter.map(transmute);

        return Promise.resolve(result);
    }

    /**
     * Updates a comment in the database using the properties in the udpate message. The filter will also include the
     * asset types to ensure that only ids within the asset collection can be manipulated.
     * @param update the message containing the id and properties to update
     * @param details the details collection in which the database entries are held
     * @param changelog the changelog into which any changes should be recorded
     * @protected
     */
    protected async updateImpl(update: UpdateCommentMessage, details: Collection<DatabaseComment>, changelog: Collection): Promise<string[]> {
        const { msg_id, msg_intention, status, id, ...manipulations } = update;

        if (!ObjectId.isValid(id)) {
            throw new Error('invalid object id');
        }

        const query: { $set: Partial<DatabaseComment> } = {
            $set: {},
        };

        if (manipulations.topic) query.$set.topic = manipulations.topic;
        if (manipulations.requiresAttention !== undefined) {
            if (manipulations.requiresAttention) {
                query.$set.requiredAttention = true;
                query.$set.attendedBy = null;
                query.$set.attendedAt = null;
            } else {
                query.$set.requiredAttention = false;
            }
        }
        if (manipulations.attendedBy) {
            query.$set.requiredAttention = false;
            query.$set.attendedBy = manipulations.attendedBy;
            query.$set.attendedAt = Date.now();
        }
        if (manipulations.body) {
            query.$set.body = manipulations.body;
        }

        const result = await details.updateOne({
            _id: new ObjectId(id),
            assetType: {
                $in: this._assetType,
            },
        }, query)

        if (result.result.ok !== 1) {
            console.error(result);
            throw new Error('failed to update');
        }

        await this.log(id, 'updated', query.$set);

        return [id];
    }

}

