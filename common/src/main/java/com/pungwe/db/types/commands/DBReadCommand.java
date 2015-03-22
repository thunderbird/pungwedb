package com.pungwe.db.types.commands;

import com.pungwe.db.types.options.DBReadOptions;

import java.util.Map;

/**
 * Created by 917903 on 19/03/2015.
 */
public class DBReadCommand<V> implements DBCommand<V, DBReadOptions> {

	final Map<String, Object> query;
	final DBReadOptions options;

	public DBReadCommand() {
		this(null, DBReadOptions.DEFAULT);
	}

	public DBReadCommand(Map<String, Object> query) {
		this(query, DBReadOptions.DEFAULT);
	}

	public DBReadCommand(Map<String, Object> query, DBReadOptions options) {
		this.query = query;
		this.options = options;
	}
}
