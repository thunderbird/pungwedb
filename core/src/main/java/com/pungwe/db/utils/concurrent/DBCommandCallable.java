package com.pungwe.db.utils.concurrent;

import com.pungwe.db.events.DBCallback;
import com.pungwe.db.types.commands.DBCommand;

import java.util.concurrent.Callable;

/**
 * Created by 917903 on 19/03/2015.
 */
public abstract class DBCommandCallable<E extends DBCommand<?, ?>, T> implements Callable<T> {

	protected final E command;
	protected final DBCallback<T> callback;

	public DBCommandCallable(E command, DBCallback<T> callback) {
		this.command = command;
		this.callback = callback;
	}

}
