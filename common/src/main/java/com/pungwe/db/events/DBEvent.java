package com.pungwe.db.events;

/**
 * Created by 917903 on 17/03/2015.
 */
public interface DBEvent<T> {

	String requestCorrelationId();
	Object target();
}
