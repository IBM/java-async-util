/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.cleversafe.util.Reference;
import org.cleversafe.util.async.AsyncReadWriteLock.ReadLockToken;
import org.cleversafe.util.async.AsyncReadWriteLock.WriteLockToken;
import org.cleversafe.util.functional.Either;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

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

      final Future<ReadLockToken> read1 = arwl.acquireReadLock();
      TestBlocking.getResult(read1, 2, TimeUnit.SECONDS);

      final Future<ReadLockToken> read2 = arwl.acquireReadLock();
      TestBlocking.getResult(read2, 2, TimeUnit.SECONDS);

      final Future<WriteLockToken> write1 = arwl.acquireWriteLock();
      Assert.assertFalse(write1.isDone());

      TestBlocking.getResult(read2).releaseReadLock();
      Assert.assertFalse(write1.isDone());

      TestBlocking.getResult(read1).releaseReadLock();
      TestBlocking.getResult(write1, 2, TimeUnit.SECONDS);

      Assert.assertFalse(arwl.acquireReadLock()
          .mapFinally(readLock -> readLock.releaseReadLock()).isDone());

      TestBlocking.getResult(write1).releaseWriteLock();
    }

    // writer, writer, reader
    {
      final AsyncReadWriteLock arwl = getReadWriteLock();

      final Future<WriteLockToken> write1 = arwl.acquireWriteLock();
      TestBlocking.getResult(write1, 2, TimeUnit.SECONDS);

      final Future<WriteLockToken> write2 = arwl.acquireWriteLock();
      Assert.assertFalse(write2.isDone());

      final Future<ReadLockToken> read1 = arwl.acquireReadLock();
      Assert.assertFalse(read1.isDone());

      TestBlocking.getResult(write1).releaseWriteLock();

      TestBlocking.getResult(
          read1.map(Either::<ReadLockToken, WriteLockToken>left)
              .or(write2.map(Either::right), Function.identity()),
          2, TimeUnit.SECONDS)
          .forEach(
              readToken -> {
                Assert.assertFalse(write2.isDone());

                readToken.releaseReadLock();
                try {
                  TestBlocking.getResult(write2, 2, TimeUnit.SECONDS).releaseWriteLock();
                } catch (final TimeoutException e) {
                  throw new AssertionError(e);
                }

              },
              writeToken -> {
                Assert.assertFalse(read1.isDone());

                writeToken.releaseWriteLock();
                try {
                  TestBlocking.getResult(read1, 2, TimeUnit.SECONDS).releaseReadLock();
                } catch (final TimeoutException e) {
                  throw new AssertionError(e);
                }
              });
    }
  }

  @Test
  public final void testReadWriteStackOverflow() throws TimeoutException {
    Assume.assumeTrue(isActuallyAsync());

    final AsyncReadWriteLock rwlock = getReadWriteLock();
    final Future<WriteLockToken> firstLock = rwlock.acquireWriteLock();
    final Future<ReadLockToken> firstRead = rwlock.acquireReadLock();
    final Reference<Future<?>> lastLock = new Reference<>(null);

    for (int i = 0; i < 100_000; i++) {
      final Future<WriteLockToken> write = rwlock.acquireWriteLock();
      final Future<ReadLockToken> read = rwlock.acquireReadLock();
      write.onComplete(WriteLockToken::releaseWriteLock);
      read.onComplete(ReadLockToken::releaseReadLock);
    }
    lastLock.set(rwlock.acquireWriteLock().mapFinally(WriteLockToken::releaseWriteLock));

    TestBlocking.getResult(firstLock).releaseWriteLock();
    TestBlocking.getResult(firstRead.mapFinally(ReadLockToken::releaseReadLock));
    TestBlocking.getResult(lastLock.get(), 15, TimeUnit.SECONDS);
  }

  @Test
  public final void testDowngradeStackOverflow() throws TimeoutException {
    Assume.assumeTrue(isActuallyAsync());

    final AsyncReadWriteLock rwlock = getReadWriteLock();
    final Future<WriteLockToken> firstLock = rwlock.acquireWriteLock();
    final Reference<Future<?>> lastLock = new Reference<>(null);

    for (int i = 0; i < 100_000; i++) {
      rwlock.acquireWriteLock()
          .onComplete(writeLockToken -> writeLockToken.downgradeLock().releaseReadLock());
    }
    lastLock.set(rwlock.acquireWriteLock().mapFinally(WriteLockToken::releaseWriteLock));

    TestBlocking.getResult(firstLock).releaseWriteLock();
    TestBlocking.getResult(lastLock.get(), 15, TimeUnit.SECONDS);
  }

  @SuppressWarnings("serial")
  private static class LockStateException extends RuntimeException {
  }

  @Test(expected = LockStateException.class)
  public final void testReadReleaseException() {
    final ReadLockToken token = TestBlocking.getResult(getReadWriteLock().acquireReadLock());

    token.releaseReadLock();
    try {
      token.releaseReadLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testWriteReleaseException() {
    final WriteLockToken token = TestBlocking.getResult(getReadWriteLock().acquireWriteLock());

    token.releaseWriteLock();
    try {
      token.releaseWriteLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testDowngradeTwiceException() {
    final WriteLockToken token = TestBlocking.getResult(getReadWriteLock().acquireWriteLock());

    token.downgradeLock();
    try {
      token.downgradeLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testDowngradeReleaseException() {
    final WriteLockToken token = TestBlocking.getResult(getReadWriteLock().acquireWriteLock());

    token.downgradeLock();
    try {
      token.releaseWriteLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }

  @Test(expected = LockStateException.class)
  public final void testDowngradeReadReleaseException() {
    final ReadLockToken token =
        TestBlocking.getResult(getReadWriteLock().acquireWriteLock()).downgradeLock();

    token.releaseReadLock();
    try {
      token.releaseReadLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    }
  }


  @Test
  public final void testDowngradeLock() throws TimeoutException {
    final AsyncReadWriteLock rwlock = getReadWriteLock();

    {
      final WriteLockToken firstLock = TestBlocking.getResult(rwlock.acquireWriteLock());
      Assert.assertFalse(rwlock.tryReadLock().isPresent());

      final ReadLockToken downgrade = firstLock.downgradeLock();

      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseReadLock();

      downgrade.releaseReadLock();
    }
    {
      final WriteLockToken firstLock = TestBlocking.getResult(rwlock.acquireWriteLock());
      Assert.assertFalse(rwlock.tryWriteLock().isPresent());

      final Future<WriteLockToken> secondLock = rwlock.acquireWriteLock();
      Assert.assertFalse(secondLock.isDone());

      final ReadLockToken downgrade = firstLock.downgradeLock();
      Assert.assertFalse(secondLock.isDone());

      downgrade.releaseReadLock();
      TestBlocking.getResult(secondLock, 2, TimeUnit.SECONDS).releaseWriteLock();
    }
  }

  @Test
  public final void testTryReadWriteLock() throws TimeoutException {
    final AsyncReadWriteLock rwlock = getReadWriteLock();
    // first read should succeed
    final ReadLockToken read = rwlock.tryReadLock().orElseThrow(AssertionError::new);
    // write lock should fail
    Assert.assertFalse(rwlock.tryWriteLock().isPresent());

    // other reads should succeed
    rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseReadLock();
    final Future<ReadLockToken> read2 =
        rwlock.acquireReadLock();
    TestBlocking.getResult(read2, 2, TimeUnit.SECONDS).releaseReadLock();

    // conventional write should also fail
    final Future<WriteLockToken> write =
        rwlock.acquireWriteLock();
    Assert.assertFalse(write.isDone());

    // release last reader
    read.releaseReadLock();

    // now writer holds the lock
    TestBlocking.getResult(write, 2, TimeUnit.SECONDS);

    // try read or write must fail
    Assert.assertFalse(rwlock.tryWriteLock().isPresent());
    Assert.assertFalse(rwlock.tryReadLock().isPresent());

    TestBlocking.getResult(write).releaseWriteLock();

    final WriteLockToken write2 = rwlock.tryWriteLock().orElseThrow(AssertionError::new);
    Assert.assertFalse(rwlock.tryReadLock().isPresent());
    final Future<ReadLockToken> read3 =
        rwlock.acquireReadLock();
    Assert.assertFalse(read3.isDone());
    final Future<WriteLockToken> write3 =
        rwlock.acquireWriteLock();
    Assert.assertFalse(write3.isDone());

    write2.releaseWriteLock();
    TestBlocking.getResult(read3, 2, TimeUnit.SECONDS);
  }

  /**
   * A group of tests for locks which ensure a fair FIFO ordering between readers and writers
   */
  public static abstract class AbstractAsyncReadWriteLockFairnessTest
      extends AbstractAsyncReadWriteLockTest {
    // a tryReadLock may not barge with fairness if a writer is waiting
    @Test
    public void testNonBargingTryRead() {
      final AsyncReadWriteLock rwlock = getReadWriteLock();
      final ReadLockToken read = TestBlocking.getResult(rwlock.acquireReadLock());
      // tryReadLock may succeed when no writers are waiting
      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseReadLock();

      final Future<WriteLockToken> write =
          rwlock.acquireWriteLock();
      // now tryReadLock should fail because a writer is waiting
      Assert.assertFalse(rwlock.tryReadLock().isPresent());

      read.releaseReadLock();
      TestBlocking.getResult(write).releaseWriteLock();
      // tryReadLock may once again proceed now that the write lock is released
      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseReadLock();
    }

    @Test
    public final void testReadWriteFairExclusivity() throws Exception {
      // reader, reader, writer
      {
        final AsyncReadWriteLock arwl = getReadWriteLock();

        final Future<ReadLockToken> read1 = arwl.acquireReadLock();
        TestBlocking.getResult(read1, 2, TimeUnit.SECONDS);

        final Future<ReadLockToken> read2 = arwl.acquireReadLock();
        TestBlocking.getResult(read2, 2, TimeUnit.SECONDS);

        final Future<WriteLockToken> write1 = arwl.acquireWriteLock();
        Assert.assertFalse(write1.isDone());

        TestBlocking.getResult(read2).releaseReadLock();
        Assert.assertFalse(write1.isDone());

        TestBlocking.getResult(read1).releaseReadLock();
        TestBlocking.getResult(write1, 2, TimeUnit.SECONDS);

        Assert.assertFalse(arwl.acquireReadLock()
            .mapFinally(readLock -> readLock.releaseReadLock()).isDone());

        TestBlocking.getResult(write1).releaseWriteLock();
      }

      // writer, writer, reader
      {
        final AsyncReadWriteLock arwl = getReadWriteLock();

        final Future<WriteLockToken> write1 = arwl.acquireWriteLock();
        TestBlocking.getResult(write1, 2, TimeUnit.SECONDS);

        final Future<WriteLockToken> write2 = arwl.acquireWriteLock();
        Assert.assertFalse(write2.isDone());

        final Future<ReadLockToken> read1 = arwl.acquireReadLock();
        Assert.assertFalse(read1.isDone());

        TestBlocking.getResult(write1).releaseWriteLock();
        TestBlocking.getResult(write2, 2, TimeUnit.SECONDS);
        Assert.assertFalse(read1.isDone());

        TestBlocking.getResult(write2).releaseWriteLock();
        TestBlocking.getResult(read1, 2, TimeUnit.SECONDS);

        TestBlocking.getResult(read1).releaseReadLock();
      }
    }

    @Test
    public final void testFairness() throws Exception {
      final AsyncReadWriteLock narwls = getReadWriteLock();

      final Future<WriteLockToken> write1 =
          narwls.acquireWriteLock();
      Assert.assertTrue(write1.isDone());

      // under write lock
      final Future<ReadLockToken> read1 =
          narwls.acquireReadLock();
      final Future<ReadLockToken> read2 =
          narwls.acquireReadLock();
      final Future<ReadLockToken> read3 =
          narwls.acquireReadLock();
      Assert.assertFalse(read1.isDone());
      Assert.assertFalse(read2.isDone());
      Assert.assertFalse(read3.isDone());

      final Future<WriteLockToken> write2 =
          narwls.acquireWriteLock();
      Assert.assertFalse(write2.isDone());

      final Future<ReadLockToken> read4 =
          narwls.acquireReadLock();
      final Future<ReadLockToken> read5 =
          narwls.acquireReadLock();
      final Future<ReadLockToken> read6 =
          narwls.acquireReadLock();
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());

      final Future<WriteLockToken> write3 =
          narwls.acquireWriteLock();
      Assert.assertFalse(write3.isDone());

      TestBlocking.getResult(write1).releaseWriteLock();
      TestBlocking.getResult(Futures.allDone(read1, read2, read3), 2, TimeUnit.SECONDS);
      Assert.assertTrue(read1.isDone());
      Assert.assertTrue(read2.isDone());
      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());

      TestBlocking.getResult(read1).releaseReadLock();
      TestBlocking.getResult(read2).releaseReadLock();
      TestBlocking.getResult(read3, 2, TimeUnit.SECONDS);
      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());

      // now under read lock (read3 still active)
      final Future<ReadLockToken> read7 =
          narwls.acquireReadLock();
      final Future<WriteLockToken> write4 =
          narwls.acquireWriteLock();

      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(read3).releaseReadLock();
      TestBlocking.getResult(write2, 2, TimeUnit.SECONDS);
      Assert.assertTrue(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(write2).releaseWriteLock();
      TestBlocking.getResult(Futures.allDone(read4, read5, read6), 2, TimeUnit.SECONDS);
      Assert.assertTrue(read4.isDone());
      Assert.assertTrue(read5.isDone());
      Assert.assertTrue(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      Arrays.asList(read4, read5, read6)
          .forEach(f -> f.onComplete(readLock -> readLock.releaseReadLock()));
      TestBlocking.getResult(write3, 2, TimeUnit.SECONDS);
      Assert.assertTrue(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(write3).releaseWriteLock();
      TestBlocking.getResult(read7, 2, TimeUnit.SECONDS);
      Assert.assertTrue(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(read7).releaseReadLock();
      TestBlocking.getResult(write4, 2, TimeUnit.SECONDS);
      Assert.assertTrue(write4.isDone());

      TestBlocking.getResult(write4).releaseWriteLock();
    }
  }

  /**
   * A group of tests for lock implementations that grant readers acquisition priority over writers
   */
  public static abstract class AbstractAsyncReadWriteLockReaderPriorityTest
      extends AbstractAsyncReadWriteLockTest {
    @Test
    public final void testReadLockBargesInReadMode() throws TimeoutException {
      final AsyncReadWriteLock rwlock = getReadWriteLock();
      // the lock is held by a reader, so new readers should never wait
      final ReadLockToken read1 = TestBlocking.getResult(rwlock.acquireReadLock(), 2, TimeUnit.SECONDS);
      final Future<WriteLockToken> write1 = rwlock.acquireWriteLock();
      Assert.assertFalse(write1.isDone());

      // barge ahead of 1 writer
      {
        final ReadLockToken bargingReader = TestBlocking.getResult(rwlock.acquireReadLock(), 2, TimeUnit.SECONDS);
        Assert.assertFalse(write1.isDone());

        final ReadLockToken bargingTryReader = rwlock.tryReadLock().orElseThrow(AssertionError::new);
        Assert.assertFalse(write1.isDone());

        bargingReader.releaseReadLock();
        bargingTryReader.releaseReadLock();
        Assert.assertFalse(write1.isDone());
      }
      // introduce a second writer
      final Future<WriteLockToken> write2 = rwlock.acquireWriteLock();
      Assert.assertFalse(write2.isDone());

      // barge ahead of 2 writers
      {
        final ReadLockToken bargingReader = TestBlocking.getResult(rwlock.acquireReadLock(), 2, TimeUnit.SECONDS);
        Assert.assertFalse(write1.isDone());
        Assert.assertFalse(write2.isDone());

        final ReadLockToken bargingTryReader = rwlock.tryReadLock().orElseThrow(AssertionError::new);
        Assert.assertFalse(write1.isDone());
        Assert.assertFalse(write2.isDone());

        bargingReader.releaseReadLock();
        bargingTryReader.releaseReadLock();
        Assert.assertFalse(write1.isDone());
        Assert.assertFalse(write2.isDone());
      }

      // sanity check cleanup
      read1.releaseReadLock();
      final WriteLockToken write1Token = TestBlocking.getResult(write1, 2, TimeUnit.SECONDS);
      Assert.assertFalse(write2.isDone());
      write1Token.releaseWriteLock();
      TestBlocking.getResult(write2, 2, TimeUnit.SECONDS).releaseWriteLock();
    }

    @Test
    public final void testReadLockBargesQueue() throws TimeoutException {
      final AsyncReadWriteLock rwlock = getReadWriteLock();
      // the lock is held by a writer, any readers will be next in the queue
      final WriteLockToken write1 = TestBlocking.getResult(rwlock.acquireWriteLock(), 2, TimeUnit.SECONDS);

      final Future<WriteLockToken> write2 = rwlock.acquireWriteLock();
      Assert.assertFalse(write2.isDone());

      final Future<ReadLockToken> bargingRead1 = rwlock.acquireReadLock();

      final Future<WriteLockToken> write3 = rwlock.acquireWriteLock();
      Assert.assertFalse(write3.isDone());

      final Future<ReadLockToken> bargingRead2 = rwlock.acquireReadLock();

      // w1 -> w2 -> r1 -> w3 -> r2 would be the order if the queue were FIFO
      // w1 -> r1 == r2 -> w2 -> w2 should be the order with reader priority

      write1.releaseWriteLock();
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(write3.isDone());
      final ReadLockToken bargingRead1Token = TestBlocking.getResult(bargingRead1, 2, TimeUnit.SECONDS);
      final ReadLockToken bargingRead2Token = TestBlocking.getResult(bargingRead2, 2, TimeUnit.SECONDS);

      // readers are now active, so new reader acquisitions should succeed
      TestBlocking.getResult(rwlock.acquireReadLock(), 2, TimeUnit.SECONDS).releaseReadLock();
      rwlock.tryReadLock().orElseThrow(AssertionError::new).releaseReadLock();

      bargingRead1Token.releaseReadLock();
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(write3.isDone());
      bargingRead2Token.releaseReadLock();

      TestBlocking.getResult(write2, 2, TimeUnit.SECONDS).releaseWriteLock();
      TestBlocking.getResult(write3, 2, TimeUnit.SECONDS).releaseWriteLock();
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
  public Future<LockToken> acquireLock() {
    return this.rwlock.acquireWriteLock().map(wt -> wt::releaseWriteLock);
  }

  @Override
  public Optional<LockToken> tryLock() {
    return this.rwlock.tryWriteLock().map(wt -> wt::releaseWriteLock);
  }
}
