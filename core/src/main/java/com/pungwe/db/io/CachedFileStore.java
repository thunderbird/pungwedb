package com.pungwe.db.io;

/**
 * Created by 917903 on 03/02/2015.
 */
public class CachedFileStore {

	private final static int DEFAULT_CACHE_SIZE = 64 * 1024 * 1024; // 64MB - minimum

	private final MemoryStore cache;
	private final Store fileStore;

	public CachedFileStore(Store fileStore) {
		this(DEFAULT_CACHE_SIZE, fileStore);
	}

	public CachedFileStore(long cacheSize, Store fileStore) {
		cache = new MemoryStore(cacheSize);
		this.fileStore = fileStore;
	}
}
