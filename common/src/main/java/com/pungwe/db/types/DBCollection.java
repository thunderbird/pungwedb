package com.pungwe.db.types;

import com.pungwe.db.concurrent.DBFuture;
import com.pungwe.db.events.DBCallback;

import java.util.Collection;
import java.util.concurrent.Future;

// FIXME: This should be entirely event based
/**
 * Created by 917903 on 12/03/2015.
 */
public interface DBCollection<D> {

	// Get One Result
	<T> DBFuture<D> get(DBReadOptions options, DBCallback<D> callback);
	<T> DBFuture<D> get(T query, DBCallback<D> callback);
	<T> DBFuture<D> get(T query, DBReadOptions options, DBCallback<D> callback);

	// Get Multiple Results
	<T> DBFuture<DBCursor<D>> list(DBCallback<DBCursor<D>> callback);
	<T> DBFuture<DBCursor<D>> list(DBReadOptions options, DBCallback<DBCursor<D>> callback);
	<T> DBFuture<DBCursor<D>> list(T query, DBCallback<DBCursor<D>> callback);
	<T> DBFuture<DBCursor<D>> list(T query, DBReadOptions options, DBCallback<DBCursor<D>> callback);

	// Insert
	DBFuture<WriteResult> insert(D object, DBCallback<D> callback);

	// Update
	<T> DBFuture<WriteResult> update(T query, D object, DBCallback<D> callback);
}
