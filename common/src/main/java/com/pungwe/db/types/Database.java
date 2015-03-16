package com.pungwe.db.types;


import java.util.Set;

// TODO: This needs to grow to include a bunch of utilities
/**
 * Created by 917903 on 12/03/2015.
 */
public interface Database<C extends DBCollection> {

	Set<String> collectionNames();
	C getCollection(String name);
	void drop();
	boolean dropCollection(String name);

}
