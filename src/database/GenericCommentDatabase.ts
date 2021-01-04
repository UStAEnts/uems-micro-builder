import { GenericMongoDatabase, MongoDBConfiguration } from "./GenericMongoDatabase";
import { CommentMessage, CommentResponse } from "@uems/uemscommlib";
import { Collection, Db, FilterQuery, ObjectId, UpdateQuery } from "mongodb";
import ReadCommentMessage = CommentMessage.ReadCommentMessage;
import DeleteCommentMessage = CommentMessage.DeleteCommentMessage;
import InternalComment = CommentResponse.InternalComment;
import ShallowInternalComment = CommentResponse.ShallowInternalComment;
import CreateCommentMessage = CommentMessage.CreateCommentMessage;
import UpdateCommentMessage = CommentMessage.UpdateCommentMessage;

export type DatabaseComment = {
    _id: string | ObjectId,
    assetType: string,
    assetID: string,
    poster: string,
    posted: number,
    category: string | null,
    requiredAttention: boolean,
    attendedBy: string | null;
    attendedAt: number | null,
    body: string,
}

export type CreateDatabaseComment = Omit<DatabaseComment, '_id'> & { _id?: string };

const transmute = (db: DatabaseComment): ShallowInternalComment => {
    return {
        assetType: db.assetType,
        id: typeof (db._id) === 'string' ? db._id : db._id.toHexString(),
        assetID: db.assetID,
        attendedBy: db.attendedBy ?? undefined,
        attendedDate: db.attendedAt ?? undefined,
        poster: db.poster,
        category: db.category ?? undefined,
        requiresAttention: db.requiredAttention,
        posted: db.posted,
        body: db.body,
    };
}

export class GenericCommentDatabase extends GenericMongoDatabase<ReadCommentMessage, CreateCommentMessage, DeleteCommentMessage, UpdateCommentMessage, ShallowInternalComment> {

    private _assetType: string[];


    constructor(assetType: string[], configuration: MongoDBConfiguration);
    constructor(assetType: string[], db: Db, collections: MongoDBConfiguration['collections']);
    constructor(assetType: string[], _configurationOrDB: MongoDBConfiguration | Db, collections?: MongoDBConfiguration['collections']) {
        super(_configurationOrDB, collections);
        this._assetType = assetType;
    }

    protected async createImpl(create: CreateCommentMessage, details: Collection, changelog: Collection): Promise<string[]> {
        if (!this._assetType.includes(create.assetType)) {
            throw new Error('Invalid asset type');
        }

        const entity: CreateDatabaseComment = {
            requiredAttention: create.requiresAttention ?? false,
            category: create.category ?? null,
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

    protected deleteImpl(remove: DeleteCommentMessage, details: Collection, changelog: Collection): Promise<string[]> {
        return super.defaultDelete(remove);
    }

    protected async queryImpl(query: ReadCommentMessage, details: Collection, changelog: Collection): Promise<ShallowInternalComment[]> {
        const exec: FilterQuery<DatabaseComment> = {};

        if (query.body) {
            exec.$text = {
                $search: query.body,
            }
        }

        if (query.requiresAttention) exec.requiredAttention = true;
        if (query.category) exec.category = query.category;
        if (query.assetType) exec.assetType = query.assetType;
        if (query.assetID) exec.assetID = query.assetID;
        if (query.posterID) exec.poster = query.posterID;
        if (query.attended) exec.attendedBy = { $exists: true, };
        if (query.posted) exec.posted = query.posted;
        if (query.id) exec._id = new ObjectId(query.id);

        const filter: DatabaseComment[] = await details.find(exec).toArray();
        const result: ShallowInternalComment[] = filter.map(transmute);

        return Promise.resolve(result);
    }

    protected async updateImpl(update: UpdateCommentMessage, details: Collection, changelog: Collection): Promise<string[]> {
        const { msg_id, msg_intention, status, id, ...manipulations } = update;

        if (!ObjectId.isValid(id)) {
            throw new Error('invalid object id');
        }

        const query: { $set: Partial<DatabaseComment> } = {
            $set: {},
        };

        if (manipulations.category) query.$set.category = manipulations.category;
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
            query.$set.attendedBy = null;
            query.$set.attendedAt = Date.now();
        }
        if (manipulations.body) {
            query.$set.body = manipulations.body;
        }

        const result = await details.updateOne({
            _id: new ObjectId(id),
        }, query)

        if (result.result.ok !== 1) {
            console.error(result);
            throw new Error('failed to update');
        }

        await this.log(id, 'updated', query.$set);

        return [id];
    }

}

