import { GenericCommentDatabase } from "../database/GenericCommentDatabase";
import { RabbitNetworkHandler } from "../messaging/GenericRabbitNetworkHandler";
import { constants } from "http2";
import { CommentMessage as CM, CommentResponse } from "@uems/uemscommlib";
import ShallowInternalComment = CommentResponse.ShallowInternalComment;
import CommentReadResponseMessage = CommentResponse.CommentReadResponseMessage;
import CommentResponseMessage = CommentResponse.CommentResponseMessage;
import CommentMessage = CM.CommentMessage;
import InternalComment = CommentResponse.InternalComment;

async function execute(
    message: CommentMessage,
    database: GenericCommentDatabase | undefined,
    send: (res: CommentResponseMessage | CommentReadResponseMessage) => void,
) {
    if (!database) {
        console.warn('query was received without a valid database connection');
        throw new Error('uninitialised database connection');
    }

    let status: number = constants.HTTP_STATUS_INTERNAL_SERVER_ERROR;
    let result: string[] | ShallowInternalComment[] = [];

    console.log('msg', message);

    try {
        switch (message.msg_intention) {
            case 'CREATE':
                result = await database.create(message);
                status = constants.HTTP_STATUS_OK;
                break;
            case 'DELETE':
                result = await database.delete(message);
                status = constants.HTTP_STATUS_OK;
                break;
            case 'READ':
                result = await database.query(message);
                status = constants.HTTP_STATUS_OK;
                break;
            case 'UPDATE':
                result = await database.update(message);
                status = constants.HTTP_STATUS_OK;
                break;
            default:
                status = constants.HTTP_STATUS_BAD_REQUEST;
        }
    } catch (e) {
        console.error('failed to query database for events', {
            error: e as unknown,
        });
    }

    if (message.msg_intention === 'READ') {
        send({
            msg_intention: message.msg_intention,
            msg_id: message.msg_id,
            userID: message.userID,
            status,
            result: result as unknown as InternalComment[],
        });
    } else {
        send({
            msg_intention: message.msg_intention,
            msg_id: message.msg_id,
            userID: message.userID,
            status,
            result: result as string[],
        });
    }
}

export default function bindCommentDatabase(
    commentRoutingPrefix: string,
    database: GenericCommentDatabase,
    broker: RabbitNetworkHandler<any, any, any, any, any, any>
): void {
    broker.on('query', (message, send, routingKey) => {
        if (routingKey === `${commentRoutingPrefix}.read`) return execute(message, database, send)
        return Promise.resolve();
    });

    broker.on('delete', (message, send, routingKey) => {
        if (routingKey === `${commentRoutingPrefix}.delete`) return execute(message, database, send)
        return Promise.resolve();
    });

    broker.on('update', (message, send, routingKey) => {
        if (routingKey === `${commentRoutingPrefix}.update`) return execute(message, database, send)
        return Promise.resolve();
    });

    broker.on('create', (message, send, routingKey) => {
        if (routingKey === `${commentRoutingPrefix}.create`) return execute(message, database, send)
        return Promise.resolve();
    });
}
