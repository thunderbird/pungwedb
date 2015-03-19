package com.pungwe.db.utils.concurrent;

import com.pungwe.db.events.DBCallback;
import com.pungwe.db.types.DBCommand;

import java.util.concurrent.Callable;

/**
 * Created by 917903 on 19/03/2015.
 */
public class DBCommandCallable<T> implements Callable<T> {

	protected final DBCommand<T> command;
	protected final DBCallback<T> callback;

	public DBCommandCallable(DBCommand<T> command, DBCallback<T> callback) {
		this.command = command;
		this.callback = callback;
	}

	@Override
	public T call() throws Exception {
		return null;
	}
}
