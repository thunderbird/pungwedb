package com.pungwe.db.events;

/**
 * Created by 917903 on 17/03/2015.
 */
public interface DBCallback<T> {

	// success
	// error
	// done
	void success(DBEvent<T> event, T object);
	void error(DBEvent<T> event, T object);

}
