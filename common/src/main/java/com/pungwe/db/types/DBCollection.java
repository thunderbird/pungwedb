package com.pungwe.db.types;

import java.util.Collection;
import java.util.concurrent.Future;

// FIXME: This should be entirely event based
/**
 * Created by 917903 on 12/03/2015.
 */
public interface DBCollection {

	Future<DBDocument> get(Object key);
	Future<DBDocument> findOne(DBDocument query);
	Future<DBCursor> find(DBDocument query);
	<T> Future<WriteResult> update(T query, DBDocument update);
	<T> Future<WriteResult> update(T query, DBDocument update, DBOptions options);
	Future<WriteResult> insert(DBDocument... docs);
	Future<WriteResult> remove(DBDocument query);

}
