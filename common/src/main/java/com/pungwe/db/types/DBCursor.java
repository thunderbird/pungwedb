package com.pungwe.db.types;

import java.util.Iterator;

/**
 * Created by 917903 on 12/03/2015.
 */
public interface DBCursor<T> extends Iterable<T>, Iterator<T> {

	String getCursorId();

}
