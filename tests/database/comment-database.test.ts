import { Db, MongoClient, ObjectId } from "mongodb";
import {
	defaultAfterAll,
	defaultAfterEach,
	defaultBeforeAll,
	defaultBeforeEach,
	haveNoAdditionalKeys,
} from "../MongoDBUtilities";
import { GenericCommentDatabase } from "../../src/database/GenericCommentDatabase";
import { CommentValidators } from "@uems/uemscommlib/build/comment/CommentValidators";
import CommentRepresentation = CommentValidators.CommentRepresentation;
import { CommentResponse } from "@uems/uemscommlib";
import ShallowInternalComment = CommentResponse.ShallowInternalComment;

describe('GenericCommentDatabase.ts', () => {

	let client!: MongoClient;
	let db!: Db;
	let commentDB!: GenericCommentDatabase;

	beforeAll(async () => {
		const { client: newClient, db: newDB } = await defaultBeforeAll();

		client = newClient;
		db = newDB;
	});

	afterAll(() => defaultAfterAll(client, db));
	afterEach(() => defaultAfterEach(client, db));

	beforeEach(() => {
		defaultBeforeEach<ShallowInternalComment>([{
			//@ts-ignore
			_id: new ObjectId('600de594349464f6f5a5fe05') as any,
			id: '600de594349464f6f5a5fe05',
			requiredAttention: false,
			topic: 'topic',
			poster: 'posterID',
			attendedAt: null,
			attendedBy: undefined,
			assetID: 'assetID',
			assetType: 'validAsset',
			posted: 1020332,
			body: 'body',
		}], client, db);
		commentDB = new GenericCommentDatabase(['validAsset'], db, {
			details: 'details',
			changelog: 'changelog',
		});
	});

	describe('create', () => {

		it('should reject creating invalid asset comments', async () => {
			await expect(commentDB.create({
				status: 0,
				msg_id: 0,
				msg_intention: 'CREATE',
				poster: 'sometihng',
				userID: 'anonymous',
				assetType: 'invalid',
				assetID: 'assetID',
				body: 'something',
				posted: Math.floor(Date.now() / 1000),
			})).rejects.toThrowError('Invalid asset type');
		});

		it('should successfully insert comments', async () => {
			const result = await commentDB.create({
				status: 0,
				msg_id: 0,
				msg_intention: 'CREATE',
				poster: 'sometihng',
				userID: 'anonymous',
				assetType: 'validAsset',
				assetID: 'assetID',
				body: 'something',
				posted: Math.floor(Date.now() / 1000),
			});

			expect(result).toHaveLength(1);
			expect(typeof (result[0])).toEqual('string');

			const results = await db.collection('details').find().toArray();
			const ids = results.map((e) => e._id.toHexString());
			expect(results).toHaveLength(2);
			expect(ids).toContain(result[0]);
			expect(ids).toContain('600de594349464f6f5a5fe05');
		});

	});

	describe('delete', () => {

		it('should support deleting comments by id', async () => {
			await expect(commentDB.delete({
				msg_intention: 'DELETE',
				msg_id: 0,
				status: 0,
				userID: 'anonymous',
				id: '600de594349464f6f5a5fe05',
			})).resolves.toEqual(['600de594349464f6f5a5fe05'])

			const results = await db.collection('details').find().toArray();
			expect(results).toHaveLength(0);
		});

		it('should reject deletes on an invalid object id', async () => {
			await expect(commentDB.delete({
				msg_intention: 'DELETE',
				msg_id: 0,
				status: 0,
				userID: 'anonymous',
				id: '("&£$(*£&^%(*&£"',
			})).rejects.toThrowError(/object ID/ig);

			const results = await db.collection('details').find().toArray();
			expect(results).toHaveLength(1);
		});

		it('should reject invalid ids', async () => {
			await expect(commentDB.delete({
				msg_intention: 'DELETE',
				msg_id: 0,
				status: 0,
				userID: 'anonymous',
				id: '600de594349464f6f5a5fe30',
			})).rejects.toThrowError(/invalid entity/ig);

			const results = await db.collection('details').find().toArray();
			expect(results).toHaveLength(1);
		});

	});

	describe('query', () => {

		it('should support querying by arbitrary properties', async () => {
			await expect(commentDB.query({
				status: 0,
				msg_id: 0,
				msg_intention: 'READ',
				userID: 'anonymous',
				topic: 'topic',
				poster: 'posterID',
				assetID: 'assetID',
				body: 'body',
			})).resolves.toHaveLength(1);
		});

		it('should return empty array on invalid search', async () => {
			await expect(commentDB.query({
				status: 0,
				msg_id: 0,
				msg_intention: 'READ',
				topic: 'topic invalid',
				userID: 'anonymous',
				poster: 'posterID',
				assetID: 'assetID',
				body: 'body',
			})).resolves.toHaveLength(0);
		});

		it('should not have any additional properties', async () => {
			const data = await commentDB.query({
				status: 0,
				msg_id: 0,
				msg_intention: 'READ',
				topic: 'topic',
				userID: 'anonymous',
				poster: 'posterID',
				assetID: 'assetID',
				body: 'body',
			});

			expect(data).toHaveLength(1);
			haveNoAdditionalKeys(data[0], [
				'assetType',
				'id',
				'assetID',
				'attendedBy',
				'attendedDate',
				'poster',
				'topic',
				'requiresAttention',
				'posted',
				'body',
			]);
		});

	});

	describe('update', function () {

		it('should not allowing adding properties via update', async () => {
			const update = await commentDB.update({
				status: 0,
				msg_intention: 'UPDATE',
				msg_id: 0,
				id: '600de594349464f6f5a5fe05',
				body: 'new body',
				// @ts-ignore
				newProperty: 'something',
			})

			expect(update).toHaveLength(1);
			expect(update).toEqual(['600de594349464f6f5a5fe05'])

			const data = await commentDB.query({
				status: 0,
				msg_id: 0,
				msg_intention: 'READ',
				topic: 'topic',
				userID: 'anonymous',
				poster: 'posterID',
				assetID: 'assetID',
			});

			expect(data).toHaveLength(1);
			haveNoAdditionalKeys(data[0], [
				'assetType',
				'id',
				'assetID',
				'attendedBy',
				'attendedDate',
				'poster',
				'topic',
				'requiresAttention',
				'posted',
				'body',
			]);
		});

		it('should support updating requiring attention', async () => {
			const update = await commentDB.update({
				status: 0,
				msg_intention: 'UPDATE',
				msg_id: 0,
				userID: 'anonymous',
				id: '600de594349464f6f5a5fe05',
				requiresAttention: true,
			})

			expect(update).toHaveLength(1);
			expect(update).toEqual(['600de594349464f6f5a5fe05'])

			const data = await commentDB.query({
				status: 0,
				msg_id: 0,
				msg_intention: 'READ',
				userID: 'anonymous',
				topic: 'topic',
				poster: 'posterID',
				assetID: 'assetID',
			});

			expect(data).toHaveLength(1);
			expect(data[0].requiresAttention).toBeTruthy();
			expect(data[0].attendedBy).toBeUndefined();
			expect(data[0].attendedDate).toBeUndefined();
		});

		it('should support updating attended by', async () => {
			const update = await commentDB.update({
				status: 0,
				userID: 'anonymous',
				msg_intention: 'UPDATE',
				msg_id: 0,
				id: '600de594349464f6f5a5fe05',
				attendedBy: 'someone',
			})

			expect(update).toHaveLength(1);
			expect(update).toEqual(['600de594349464f6f5a5fe05'])

			const data = await commentDB.query({
				status: 0,
				msg_id: 0,
				userID: 'anonymous',
				msg_intention: 'READ',
				topic: 'topic',
				poster: 'posterID',
				assetID: 'assetID',
			});

			expect(data).toHaveLength(1);
			expect(data[0].requiresAttention).toBeFalsy();
			expect(data[0].attendedBy).toEqual('someone');
			expect(data[0].attendedDate).not.toBeUndefined();
		});

		it('should support updating arbitrary properties', async () => {
			const update = await commentDB.update({
				status: 0,
				msg_intention: 'UPDATE',
				userID: 'anonymous',
				msg_id: 0,
				id: '600de594349464f6f5a5fe05',
				body: 'new body for test',
			})

			expect(update).toHaveLength(1);
			expect(update).toEqual(['600de594349464f6f5a5fe05'])

			const data = await commentDB.query({
				status: 0,
				msg_id: 0,
				msg_intention: 'READ',
				userID: 'anonymous',
				topic: 'topic',
				poster: 'posterID',
				assetID: 'assetID',
			});

			expect(data).toHaveLength(1);
			expect(data[0].body).toEqual('new body for test');
		});

	});

});
