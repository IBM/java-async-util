//
// (C) Copyright IBM Corp. 2005 All Rights Reserved.
//
// Contact Information:
//
// IBM Corporation
// Legal Department
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// END-OF-HEADER
//
// -----------------------
// @author: renar
//
// Date: Nov 15, 2016
// ---------------------

package com.ibm.async_util.locks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.ibm.async_util.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.async_util.locks.AsyncStampedLock.Stamp;

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

    readLockToken.releaseReadLock();
    final AsyncReadWriteLock.WriteLockToken writeLockToken = TestUtil.join(writeLock);

    Assert.assertFalse(stamp0.validate());
    Assert.assertFalse(stamp1.validate());
    final Stamp stamp2 = asl.tryOptimisticRead();
    Assert.assertNull(stamp2);

    writeLockToken.releaseWriteLock();

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
