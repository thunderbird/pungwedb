package com.pungwe.db.types.btree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by 917903 on 04/03/2015.
 */
final class BTreeEntry<K, V> implements Map.Entry<K, V> {

	static final Logger log = LoggerFactory.getLogger(BTreeEntry.class);

	private final BTreeMap<K, V> map;
	private final Object key, value;

	public BTreeEntry(Object key, Object value, BTreeMap<K, V> map) {
		this.key = key;
		this.value = value;
		this.map = map;
	}

	@Override
	public K getKey() {
		return (K) key;
	}

	@Override
	public V getValue() {
		try {
			if (map.referenced) {
				return map.store.get((Long) value, map.valueSerializer);
			} else {
				return (V) value;
			}
		} catch (IOException ex) {
			log.error("Error reading value", ex);
			return null;
		}
	}

	@Override
	public V setValue(V value) {
		return map.put2((K) key, (V) value, true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BTreeEntry that = (BTreeEntry) o;

		if (!key.equals(that.key)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}
}
