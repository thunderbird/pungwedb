package com.pungwe.db.concurrent;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by 917903 on 17/03/2015.
 */
public interface DBFuture<T> extends Future<T> {

	DBFuture<T> waitUntilDone() throws TimeoutException;
	DBFuture<T> waitUntilDone(long time, TimeUnit t);

}
