package com.pungwe.db.types.btree;

import java.util.*;

/**
 * Created by 917903 on 04/03/2015.
 */
final class DescendingKeySet<E> extends AbstractSet<E> implements NavigableSet<E> {

	final BTreeMap<E, Object> map;
	final Object lo, hi;
	final boolean loInclusive, hiInclusive;

	public DescendingKeySet(BTreeMap<E, Object> map) {
		this.map = map;
		lo = null;
		hi = null;
		loInclusive = false;
		hiInclusive = false;
	}

	public DescendingKeySet(BTreeMap<E, Object> map, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		this.map = map;
		this.hi = hi;
		this.lo = lo;
		this.hiInclusive = hiInclusive;
		this.loInclusive = loInclusive;
	}

	@Override
	public Iterator<E> iterator() {
		return new KeyIterator<E>(map.descendingIterator(lo, loInclusive, hi, hiInclusive));
	}

	@Override
	public Iterator<E> descendingIterator() {
		return new KeyIterator<E>(map.entryIterator(hi, hiInclusive, lo, loInclusive));
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
