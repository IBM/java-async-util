/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.asyncutil.util.Reference;
import com.ibm.asyncutil.util.TestUtil;

public abstract class AbstractAsyncReadWriteLockTest extends AbstractAsyncLockTest {

  protected abstract AsyncReadWriteLock getReadWriteLock();

  @Override
  protected final AsyncLock getLock() {
    return new RWLockAsAsyncLock(getReadWriteLock());
  }

  @Test
  public final void testReadWriteExclusivity() throws Exception {
    // reader, reader, writer
    {
      final AsyncReadWriteLock arwl = getReadWriteLock();

      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read1 =
          arwl.acquireReadLock().toCompletableFuture();
      TestUtil.join(read1, 2, TimeUnit.SECONDS);

      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read2 =
          arwl.acquireReadLock().toCompletableFuture();
      TestUtil.join(read2, 2, TimeUnit.SECONDS);

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write1 =
          arwl.acquireWriteLock().toCompletableFuture();
      Assert.assertFalse(write1.isDone());

      TestUtil.join(read2).releaseLock();
      Assert.assertFalse(write1.isDone());

      TestUtil.join(read1).releaseLock();
      TestUtil.join(write1, 2, TimeUnit.SECONDS);

      Assert.assertFalse(arwl.acquireReadLock().thenAccept(readLock -> readLock.releaseLock())
          .toCompletableFuture().isDone());

      TestUtil.join(write1).releaseLock();
    }

    // writer, writer, reader
    {
      final AsyncReadWriteLock arwl = getReadWriteLock();

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write1 =
          arwl.acquireWriteLock().toCompletableFuture();
      TestUtil.join(write1, 2, TimeUnit.SECONDS);

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write2 =
          arwl.acquireWriteLock().toCompletableFuture();
      Assert.assertFalse(write2.isDone());

      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read1 =
          arwl.acquireReadLock().toCompletableFuture();
      Assert.assertFalse(read1.isDone());

      TestUtil.join(write1).releaseLock();
      TestUtil.join(write2, 2, TimeUnit.SECONDS);
      Assert.assertFalse(read1.isDone());

      TestUtil.join(write2).releaseLock();
      TestUtil.join(read1, 2, TimeUnit.SECONDS);

      TestUtil.join(read1).releaseLock();
    }
  }

  @Test
  public final void testReadWriteStackOverflow() throws TimeoutException {
    Assume.assumeTrue(isActuallyAsync());

    final AsyncReadWriteLock rwlock = getReadWriteLock();
    final CompletableFuture<AsyncReadWriteLock.WriteLockToken> firstLock =
        rwlock.acquireWriteLock().toCompletableFuture();
    final CompletableFuture<AsyncReadWriteLock.ReadLockToken> firstRead =
        rwlock.acquireReadLock().toCompletableFuture();
    final Reference<CompletionStage<?>> lastLock = new Reference<>(null);

    for (int i = 0; i < 100_000; i++) {
      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write =
          rwlock.acquireWriteLock().toCompletableFuture();
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read =
          rwlock.acquireReadLock().toCompletableFuture();
      write.thenAccept(writeLockToken -> {
        writeLockToken.releaseLock();
        read.thenAccept(AsyncReadWriteLock.ReadLockToken::releaseLock);
      });
    }
    lastLock.set(rwlock.acquireWriteLock().thenApply(writeToken -> {
      writeToken.releaseLock();
      return writeToken;
    }));

    TestUtil.join(firstLock).releaseLock();
    TestUtil.join(firstRead.thenAccept(AsyncReadWriteLock.ReadLockToken::releaseLock));
    TestUtil.join(lastLock.get(), 15, TimeUnit.SECONDS);
  }

  @Test
  public final void testDowngradeStackOverflow() throws TimeoutException {
    Assume.assumeTrue(isActuallyAsync());

    final AsyncReadWriteLock rwlock = getReadWriteLock();
    final CompletableFuture<AsyncReadWriteLock.WriteLockToken> firstLock =
        rwlock.acquireWriteLock().toCompletableFuture();
    final Reference<CompletionStage<?>> lastLock = new Reference<>(null);

    for (int i = 0; i < 100_000; i++) {
      rwlock.acquireWriteLock()
          .thenAccept(writeLockToken -> writeLockToken.downgradeLock().releaseLock());
    }
    lastLock.set(rwlock.acquireWriteLock().thenApply(writeToken -> {
      writeToken.releaseLock();
      return writeToken;
    }));

    TestUtil.join(firstLock).releaseLock();
    TestUtil.join(lastLock.get(), 15, TimeUnit.SECONDS);
  }

  @SuppressWarnings("serial")
  private static class LockStateException extends RuntimeException {
  }

  @Test(expected = LockStateException.class)
  public final void testReadReleaseException() {
    final AsyncReadWriteLock.ReadLockToken token =
        TestUtil.join(getReadWriteLock().acquireReadLock());

    token.releaseLock();
    try {
      token.releaseLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testWriteReleaseException() {
    final AsyncReadWriteLock.WriteLockToken token =
        TestUtil.join(getReadWriteLock().acquireWriteLock());

    token.releaseLock();
    try {
      token.releaseLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testDowngradeTwiceException() {
    final AsyncReadWriteLock.WriteLockToken token =
        TestUtil.join(getReadWriteLock().acquireWriteLock());

    token.downgradeLock();
    try {
      token.downgradeLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testDowngradeReleaseException() {
    final AsyncReadWriteLock.WriteLockToken token =
        TestUtil.join(getReadWriteLock().acquireWriteLock());

    token.downgradeLock();
    try {
      token.releaseLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testDowngradeReadReleaseException() {
    final AsyncReadWriteLock.ReadLockToken token =
        TestUtil.join(getReadWriteLock().acquireWriteLock()).downgradeLock();

    token.releaseLock();
    try {
      token.releaseLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }


  @Test
  public final void testDowngradeLock() throws TimeoutException {
    final AsyncReadWriteLock rwlock = getReadWriteLock();

    {
      final AsyncReadWriteLock.WriteLockToken firstLock = TestUtil.join(rwlock.acquireWriteLock());
      Assert.assertFalse(rwlock.tryReadLock().isPresent());

      final AsyncReadWriteLock.ReadLockToken downgrade = firstLock.downgradeLock();

      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseLock();

      downgrade.releaseLock();
    }
    {
      final AsyncReadWriteLock.WriteLockToken firstLock = TestUtil.join(rwlock.acquireWriteLock());
      Assert.assertFalse(rwlock.tryWriteLock().isPresent());

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> secondLock =
          rwlock.acquireWriteLock().toCompletableFuture();
      Assert.assertFalse(secondLock.isDone());

      final AsyncReadWriteLock.ReadLockToken downgrade = firstLock.downgradeLock();
      Assert.assertFalse(secondLock.isDone());

      downgrade.releaseLock();
      TestUtil.join(secondLock, 2, TimeUnit.SECONDS).releaseLock();
    }
  }

  @Test
  public final void testTryReadWriteLock() throws TimeoutException {
    final AsyncReadWriteLock rwlock = getReadWriteLock();
    // first read should succeed
    final AsyncReadWriteLock.ReadLockToken read =
        rwlock.tryReadLock().orElseThrow(AssertionError::new);
    // write lock should fail
    Assert.assertFalse(rwlock.tryWriteLock().isPresent());

    // other reads should succeed
    rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseLock();
    final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read2 =
        rwlock.acquireReadLock().toCompletableFuture();
    TestUtil.join(read2, 2, TimeUnit.SECONDS).releaseLock();

    // conventional write should also fail
    final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write =
        rwlock.acquireWriteLock().toCompletableFuture();
    Assert.assertFalse(write.isDone());

    // release last reader
    read.releaseLock();

    // now writer holds the lock
    TestUtil.join(write, 2, TimeUnit.SECONDS);

    // try read or write must fail
    Assert.assertFalse(rwlock.tryWriteLock().isPresent());
    Assert.assertFalse(rwlock.tryReadLock().isPresent());

    TestUtil.join(write).releaseLock();

    final AsyncReadWriteLock.WriteLockToken write2 =
        rwlock.tryWriteLock().orElseThrow(AssertionError::new);
    Assert.assertFalse(rwlock.tryReadLock().isPresent());
    final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read3 =
        rwlock.acquireReadLock().toCompletableFuture();
    Assert.assertFalse(read3.isDone());
    final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write3 =
        rwlock.acquireWriteLock().toCompletableFuture();
    Assert.assertFalse(write3.isDone());

    write2.releaseLock();
    TestUtil.join(read3, 2, TimeUnit.SECONDS);
  }


  public static abstract class AbstractAsyncReadWriteLockFairnessTest
      extends AbstractAsyncReadWriteLockTest {
    // a tryReadLock may not barge with fairness if a writer is waiting
    @Test
    public void testNonBargingTryRead() {
      final AsyncReadWriteLock rwlock = getReadWriteLock();
      final AsyncReadWriteLock.ReadLockToken read = TestUtil.join(rwlock.acquireReadLock());
      // tryReadLock may succeed when no writers are waiting
      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseLock();

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write =
          rwlock.acquireWriteLock().toCompletableFuture();
      // now tryReadLock should fail because a writer is waiting
      Assert.assertFalse(rwlock.tryReadLock().isPresent());

      read.releaseLock();
      TestUtil.join(write).releaseLock();
      // tryReadLock may once again proceed now that the write lock is released
      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseLock();
    }

    @Test
    public final void testFairness() throws Exception {
      final AsyncReadWriteLock narwls = getReadWriteLock();

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write1 =
          narwls.acquireWriteLock().toCompletableFuture();
      Assert.assertTrue(write1.isDone());

      // under write lock
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read1 =
          narwls.acquireReadLock().toCompletableFuture();
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read2 =
          narwls.acquireReadLock().toCompletableFuture();
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read3 =
          narwls.acquireReadLock().toCompletableFuture();
      Assert.assertFalse(read1.isDone());
      Assert.assertFalse(read2.isDone());
      Assert.assertFalse(read3.isDone());

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write2 =
          narwls.acquireWriteLock().toCompletableFuture();
      Assert.assertFalse(write2.isDone());

      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read4 =
          narwls.acquireReadLock().toCompletableFuture();
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read5 =
          narwls.acquireReadLock().toCompletableFuture();
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read6 =
          narwls.acquireReadLock().toCompletableFuture();
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());

      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write3 =
          narwls.acquireWriteLock().toCompletableFuture();
      Assert.assertFalse(write3.isDone());

      TestUtil.join(write1).releaseLock();
      TestUtil.join(CompletableFuture.allOf(read1, read2, read3), 2, TimeUnit.SECONDS);
      Assert.assertTrue(read1.isDone());
      Assert.assertTrue(read2.isDone());
      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());

      TestUtil.join(read1).releaseLock();
      TestUtil.join(read2).releaseLock();
      TestUtil.join(read3, 2, TimeUnit.SECONDS);
      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());

      // now under read lock (read3 still active)
      final CompletableFuture<AsyncReadWriteLock.ReadLockToken> read7 =
          narwls.acquireReadLock().toCompletableFuture();
      final CompletableFuture<AsyncReadWriteLock.WriteLockToken> write4 =
          narwls.acquireWriteLock().toCompletableFuture();

      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestUtil.join(read3).releaseLock();
      TestUtil.join(write2, 2, TimeUnit.SECONDS);
      Assert.assertTrue(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestUtil.join(write2).releaseLock();
      TestUtil.join(CompletableFuture.allOf(read4, read5, read6), 2, TimeUnit.SECONDS);
      Assert.assertTrue(read4.isDone());
      Assert.assertTrue(read5.isDone());
      Assert.assertTrue(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      Arrays.asList(read4, read5, read6)
          .forEach(f -> f.thenAccept(readLock -> readLock.releaseLock()));
      TestUtil.join(write3, 2, TimeUnit.SECONDS);
      Assert.assertTrue(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestUtil.join(write3).releaseLock();
      TestUtil.join(read7, 2, TimeUnit.SECONDS);
      Assert.assertTrue(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestUtil.join(read7).releaseLock();
      TestUtil.join(write4, 2, TimeUnit.SECONDS);
      Assert.assertTrue(write4.isDone());

      TestUtil.join(write4).releaseLock();
    }
  }

  public static class SyncAsyncReadWriteLockTest extends AbstractAsyncReadWriteLockFairnessTest {
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
    public void testNonBargingTryRead() {
      // doesn't work well with executor delegation. ignore the test
    }

    @Override
    protected AsyncReadWriteLock getReadWriteLock() {
      return new SemaphoreAsAsyncReadWriteLock(
          permits -> new SyncAsyncSemaphore(permits, true, pool), Integer.MAX_VALUE);
    }
  }
}


class RWLockAsAsyncLock implements AsyncLock {
  private final AsyncReadWriteLock rwlock;

  public RWLockAsAsyncLock(final AsyncReadWriteLock rwlock) {
    this.rwlock = rwlock;
  }

  @Override
  public CompletionStage<LockToken> acquireLock() {
    return this.rwlock.acquireWriteLock().thenApply(wt -> wt::releaseLock);
  }

  @Override
  public Optional<LockToken> tryLock() {
    return this.rwlock.tryWriteLock().map(wt -> wt::releaseLock);
  }
}
