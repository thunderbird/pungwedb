package com.pungwe.db.types.document;

import com.pungwe.db.concurrent.DBFuture;
import com.pungwe.db.events.DBCallback;
import com.pungwe.db.types.*;

import java.util.concurrent.ExecutorService;

/**
 * Created by 917903 on 19/03/2015.
 */
public class DBDocumentCollection implements DBCollection<DBDocument> {

	ExecutorService executorService;

	@Override
	public <T> DBFuture<DBDocument> get(DBReadOptions options, DBCallback<DBDocument> callback) {

		return null;
	}

	@Override
	public <T> DBFuture<DBDocument> get(T query, DBCallback<DBDocument> callback) {
		return null;
	}

	@Override
	public <T> DBFuture<DBDocument> get(T query, DBReadOptions options, DBCallback<DBDocument> callback) {
		return null;
	}

	@Override
	public <T> DBFuture<DBCursor<DBDocument>> list(DBCallback<DBCursor<DBDocument>> callback) {
		return null;
	}

	@Override
	public <T> DBFuture<DBCursor<DBDocument>> list(DBReadOptions options, DBCallback<DBCursor<DBDocument>> callback) {
		return null;
	}

	@Override
	public <T> DBFuture<DBCursor<DBDocument>> list(T query, DBCallback<DBCursor<DBDocument>> callback) {
		return null;
	}

	@Override
	public <T> DBFuture<DBCursor<DBDocument>> list(T query, DBReadOptions options, DBCallback<DBCursor<DBDocument>> callback) {
		return null;
	}

	@Override
	public DBFuture<WriteResult> insert(DBDocument object, DBCallback<DBDocument> callback) {
		return null;
	}

	@Override
	public <T> DBFuture<WriteResult> update(T query, DBDocument object, DBCallback<DBDocument> callback) {
		return null;
	}

}
