package com.pungwe.db.util.collections;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by 917903 on 24/03/2015.
 */
public abstract class AbstractLargeCollection<T> extends AbstractCollection<T> implements LargeCollection<T> {

	@Override
	public int size() {
		return (int)Math.min(sizeLong(), Integer.MAX_VALUE);
	}

}
