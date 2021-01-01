export interface GenericDatabase<READ, CREATE, DELETE, UPDATE, REPRESENTATION> {

    /**
     * Queries the database for entities matching a set of critera. Returns an array of matching events in a promise like
     * format
     * @param query the query received by the system
     */
    query(query: READ): Promise<REPRESENTATION[]> | PromiseLike<REPRESENTATION[]>;

    /**
     * Creates a new entity in the data store with the provided property. Returns an array of IDs of the created
     * resources
     * @param create the create instruction received by the system
     */
    create(create: CREATE): Promise<string[]> | PromiseLike<string[]>;

    /**
     * Deletes an existing entity in the data store with the provided id. Returns an array of the ids adjusted by this
     * instruction
     * @param del the delete instruction received by the system
     */
    delete(del: DELETE): Promise<string[]> | PromiseLike<string[]>;

    /**
     * Updates an existing entity in the data store with the provided updates. Returns an array of the ids of adjusted
     * objects by this instruction
     * @param update the update instruction received by the system
     */
    update(update: UPDATE): Promise<string[]> | PromiseLike<string[]>;

}

/**
 * An interface describing the events and handlers for the database class
 */
export interface DatabaseEvents {
    /**
     * Emitted when the class has fully connected to the database and is ready to work
     */
    ready: () => void,
    /**
     * Emitted when there is an error raised during operation. Parameter is the error raised if appropriate or
     * undefined. As the errors can technically be any type is is left as unknown.
     * @param err the error raised by the erroring region or undefined if none is provided
     */
    error: (err: unknown) => void,
}
