package com.pungwe.db.types.btree;

import java.util.*;

/**
 * Created by 917903 on 04/03/2015.
 */
final class KeySet<E> extends AbstractSet<E> implements NavigableSet<E> {

	final BTreeMap<E, ?> map;

	public KeySet(BTreeMap<E, ?> map) {
		this.map = map;
	}

	@Override
	public Iterator<E> iterator() {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public E lower(E e) {
		return null;
	}

	@Override
	public E floor(E e) {
		return null;
	}

	@Override
	public E ceiling(E e) {
		return null;
	}

	@Override
	public E higher(E e) {
		return null;
	}

	@Override
	public E pollFirst() {
		return null;
	}

	@Override
	public E pollLast() {
		return null;
	}

	@Override
	public NavigableSet<E> descendingSet() {
		return null;
	}

	@Override
	public Iterator<E> descendingIterator() {
		return null;
	}

	@Override
	public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		return null;
	}

	@Override
	public NavigableSet<E> headSet(E toElement, boolean inclusive) {
		return null;
	}

	@Override
	public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
		return null;
	}

	@Override
	public SortedSet<E> subSet(E fromElement, E toElement) {
		return null;
	}

	@Override
	public SortedSet<E> headSet(E toElement) {
		return null;
	}

	@Override
	public SortedSet<E> tailSet(E fromElement) {
		return null;
	}

	@Override
	public Comparator<? super E> comparator() {
		return null;
	}

	@Override
	public E first() {
		return null;
	}

	@Override
	public E last() {
		return null;
	}
}
