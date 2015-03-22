package com.pungwe.db.types;

import com.pungwe.db.concurrent.DBFuture;
import com.pungwe.db.events.DBCallback;
import com.pungwe.db.types.options.DBInsertOptions;
import com.pungwe.db.types.options.DBReadOptions;
import com.pungwe.db.types.options.DBUpdateOptions;

// FIXME: This should be entirely event based
/**
 * Created by 917903 on 12/03/2015.
 */
public interface DBCollection<D> {

	// Get One Result
	default DBFuture<D> get(DBCallback<D> callback) {
		return get(DBReadOptions.DEFAULT, callback);
	}

	default DBFuture<D> get(DBReadOptions options, DBCallback<D> callback) {
		return get(null, options, callback);
	}

	default <T> DBFuture<D> get(T query, DBCallback<D> callback) {
		return get(query, DBReadOptions.DEFAULT, callback);
	}

	<T> DBFuture<D> get(T query, DBReadOptions options, DBCallback<D> callback);

	// Get Multiple Results
	default DBFuture<DBCursor<D>> list(DBCallback<DBCursor<D>> callback) {
		return list(DBReadOptions.DEFAULT, callback);
	}

	default DBFuture<DBCursor<D>> list(DBReadOptions options, DBCallback<DBCursor<D>> callback) {
		return list(null, options, callback);
	}

	default <T> DBFuture<DBCursor<D>> list(T query, DBCallback<DBCursor<D>> callback) {
		return list(null, DBReadOptions.DEFAULT, callback);
	}

	<T> DBFuture<DBCursor<D>> list(T query, DBReadOptions options, DBCallback<DBCursor<D>> callback);

	// Insert
	default DBFuture<WriteResult> insert(D object, DBCallback<D> callback) {
		return insert(object, DBInsertOptions.DEFAULT, callback);
	}

	DBFuture<WriteResult> insert(D object, DBInsertOptions options, DBCallback<D> callback);

	// Update
	default DBFuture<WriteResult> update(D object, DBCallback<D> callback) {
		return update(null, object, callback);
	}

	default DBFuture<WriteResult> update(D object, DBUpdateOptions options, DBCallback<D> callback) {
		return update(null, object, options, callback);
	}

	default <T> DBFuture<WriteResult> update(T query, D object, DBCallback<D> callback) {
		return update(query, object, DBUpdateOptions.DEFAULT, callback);
	}

	<T> DBFuture<WriteResult> update(T query, D object, DBUpdateOptions update, DBCallback<D> callback);
}
