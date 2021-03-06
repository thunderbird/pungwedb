package com.pungwe.db.types.options;

/**
 * Created by 917903 on 12/03/2015.
 */
public final class DBUpdateOptions implements DBCommandOptions {

	public static final DBUpdateOptions DEFAULT = new DBUpdateOptions();

	boolean returnNew = false;
	boolean updateMulti = false;

	public final DBUpdateOptions returnNew(boolean arg0) {
		this.returnNew = arg0;
		return this;
	}

	public final DBUpdateOptions updateMulti(boolean arg0) {
		this.updateMulti = arg0;
		return this;
	}
}
