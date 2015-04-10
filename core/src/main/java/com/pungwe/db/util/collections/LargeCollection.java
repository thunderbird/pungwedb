package com.pungwe.db.util.collections;

import java.util.Collection;

/**
 * Created by 917903 on 24/03/2015.
 */
public interface LargeCollection<T> extends Collection<T> {

	@Override
	default int size() {
		return (int)Math.min(sizeLong(), Integer.MAX_VALUE);
	}

	long sizeLong();

	@Override
	default Object[] toArray() {
		return toArray(0, true, sizeLong(), false);
	}

	Object[] toArray(long from, boolean fromInclusive, long to, boolean toInclusive);

	default Object[] toHeadArray(long to, boolean inclusive) {
		return toArray(0, true, to, inclusive);
	}

	default Object[] toTailArray(long from, boolean inclusive) {
		return toArray(from, inclusive, sizeLong(), false);
	}
}
