/**
 * Represents an error which can be displayed directly to the user, this means that it contains no information which can
 * compromise user information or the system
 */
export class ClientFacingError extends Error {

    constructor(message: string) {
        super(message);
    }
}
