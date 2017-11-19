/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.asyncutil.locks.AsyncStampedLock.Stamp;
import com.ibm.asyncutil.util.TestUtil;

public abstract class AbstractAsyncStampedLockTest extends AbstractAsyncReadWriteLockTest {

  @Override
  protected final AsyncReadWriteLock getReadWriteLock() {
    return createLock();
  }

  protected abstract AsyncStampedLock createLock();

  @Test
  public void testOptimism() {
    final AsyncStampedLock asl = createLock();

    final Stamp stamp0 = asl.tryOptimisticRead();

    Assert.assertNotNull(stamp0);
    Assert.assertTrue(stamp0.validate());

    final AsyncReadWriteLock.ReadLockToken readLockToken = TestUtil.join(asl.acquireReadLock());

    Assert.assertTrue(stamp0.validate());

    final Stamp stamp1 = asl.tryOptimisticRead();
    Assert.assertNotNull(stamp1);
    Assert.assertTrue(stamp1.validate());

    final CompletableFuture<AsyncReadWriteLock.WriteLockToken> writeLock =
        asl.acquireWriteLock().toCompletableFuture();

    // stamps are still valid because the write lock has not yet acquired
    Assert.assertTrue(stamp0.validate());
    Assert.assertTrue(stamp1.validate());

    readLockToken.releaseLock();
    final AsyncReadWriteLock.WriteLockToken writeLockToken = TestUtil.join(writeLock);

    Assert.assertFalse(stamp0.validate());
    Assert.assertFalse(stamp1.validate());
    final Stamp stamp2 = asl.tryOptimisticRead();
    Assert.assertNull(stamp2);

    writeLockToken.releaseLock();

    final Stamp stamp3 = asl.tryOptimisticRead();
    Assert.assertNotNull(stamp3);
    Assert.assertTrue(stamp3.validate());

    Assert.assertFalse(stamp0.validate());
    Assert.assertFalse(stamp1.validate());
  }


  public static class SyncAsyncStampedLockTest extends AbstractAsyncStampedLockTest {
    @Override
    protected boolean isActuallyAsync() {
      return false;
    }

    private static ExecutorService pool;

    @BeforeClass
    public static void setupPool() {
      pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @AfterClass
    public static void shutdownPool() throws InterruptedException {
      pool.shutdown();
      pool.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Override
    protected SyncAsyncStampedLock createLock() {
      return new SyncAsyncStampedLock(pool);
    }
  }
}
