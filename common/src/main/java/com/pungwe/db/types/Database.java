package com.pungwe.db.types;


import com.pungwe.db.types.options.DBCollectionOptions;

import java.util.Set;

// TODO: This needs to grow to include a bunch of utilities
/**
 * Created by 917903 on 12/03/2015.
 */
public interface Database<C extends DBCollection> {

	Set<String> collectionNames();

	/**
	 * Gets a collection by it's name. If it does not exist; then it is created with the default options.
	 * @param name The name of the collection
	 * @return the requested collection
	 */
	C getCollection(String name);

	/**
	 * Gets a collection by it's name. If it does not exist; then it is created with the options specified. If it
	 * does exist; then the collection options are ignored and instead the stored collection options are used.
	 * @param name The name of the collection
	 * @param options The options to use for creation of the collection
	 * @return the requested collection
	 */
	C getCollection(String name, DBCollectionOptions options);

	/**
	 * Drops the database and deletes all it's files on all systems.
	 */
	void drop();

	/**
	 * Drops the named collection
	 * @param name the name of the collection to drop
	 * @return true if it was dropped, false if not.
	 */
	boolean dropCollection(String name);

}
