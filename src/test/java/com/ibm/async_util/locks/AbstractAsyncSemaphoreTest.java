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
// Date: Aug 4, 2016
// ---------------------

package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.async_util.util.TestUtil;

public abstract class AbstractAsyncSemaphoreTest extends AbstractAsyncReadWriteLockTest {
  private final long maxPermits;

  AbstractAsyncSemaphoreTest(final long maxPermits) {
    this.maxPermits = maxPermits;
  }

  protected abstract AsyncSemaphore createSemaphore(long initialPermits);

  protected boolean supportsLongArgs() {
    return true;
  }

  @Override
  protected final AsyncReadWriteLock getReadWriteLock() {
    return new SemaphoreAsAsyncReadWriteLock(this::createSemaphore, this.maxPermits);
  }

  // longs that are just outside the bounds of integer range
  private static final long smallLong = Integer.MAX_VALUE + 1L;
  private static final long smallNegativeLong = Integer.MIN_VALUE - 1L;
  static {
    assert smallLong > 0L;
    assert smallNegativeLong < 0L;
  }

  @Test
  public final void constructor() {
    final int permits = 99;
    final AsyncSemaphore t1 = createSemaphore(permits);
    Assert.assertEquals(permits, t1.getAvailablePermits());
  }

  @Test
  public final void constructorLong() {
    Assume.assumeTrue(supportsLongArgs());

    final AsyncSemaphore t1 = createSemaphore(smallLong);
    Assert.assertEquals(smallLong, t1.getAvailablePermits());
  }

  @Test
  public final void constructorNegative() {
    final int permits = -1;
    Assert.assertTrue(permits < 0);
    final AsyncSemaphore t1 = createSemaphore(permits);
    Assert.assertEquals(permits, t1.getAvailablePermits());
  }

  @Test
  public final void constructorLongNegative() {
    Assume.assumeTrue(supportsLongArgs());

    final AsyncSemaphore t1 = createSemaphore(smallNegativeLong);
    Assert.assertEquals(smallNegativeLong, t1.getAvailablePermits());
  }

  @Test(expected = IllegalArgumentException.class)
  public void failNegativeAcquire() {
    createSemaphore(0).acquire(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failNegativeTryAcquire() {
    createSemaphore(0).tryAcquire(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failNegativeRelease() {
    createSemaphore(0).release(-1);
  }

  @Test
  public final void testLongArgs() {
    Assume.assumeTrue(supportsLongArgs());

    final AsyncSemaphore as = createSemaphore(smallLong + 1);
    TestUtil.join(as.acquire(smallLong));
    Assert.assertEquals(1, as.getAvailablePermits());
    as.release(smallLong + 2);
    Assert.assertTrue(as.tryAcquire(smallLong + 1));
    Assert.assertEquals(2, as.getAvailablePermits());
  }

  @Test
  public void testTryAcquire() {
    final AsyncSemaphore as = createSemaphore(0);
    Assert.assertFalse(as.tryAcquire(1L));

    Assume.assumeTrue(supportsLongArgs());

    Assert.assertFalse(as.tryAcquire(smallLong));
    Assert.assertEquals(0, as.getAvailablePermits());
    as.release(10);
    Assert.assertTrue(as.tryAcquire(1L));
    Assert.assertFalse(as.tryAcquire(smallLong));
    Assert.assertEquals(9, as.getAvailablePermits());
    as.release(smallLong);
    Assert.assertTrue(as.tryAcquire(smallLong + 4));
    Assert.assertEquals(5, as.getAvailablePermits());
  }

  @Test
  public void testSimple() throws Exception {
    final AsyncSemaphore as = createSemaphore(10);

    {
      final CompletionStage<Void> acquire = as.acquire(1);
      TestUtil.join(acquire, 1, TimeUnit.SECONDS);
      Assert.assertEquals(9, as.getAvailablePermits());
    }

    as.release(1);
    Assert.assertEquals(10, as.getAvailablePermits());

    {
      final CompletableFuture<Void> acquire = as.acquire(12).toCompletableFuture();
      Assert.assertFalse(acquire.isDone());

      as.release(1);
      Assert.assertFalse(acquire.isDone());
      as.release(1);
      TestUtil.join(acquire, 1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testDrain() throws Exception {
    final AsyncSemaphore as = createSemaphore(10);

    final CompletionStage<Void> acquire = as.acquire(1);
    TestUtil.join(acquire, 1, TimeUnit.SECONDS);
    Assert.assertEquals(9, as.getAvailablePermits());

    Assert.assertEquals(9, as.drainPermits());
    Assert.assertEquals(0, as.getAvailablePermits());
    Assert.assertEquals(0, as.drainPermits());
    Assert.assertEquals(0, as.getAvailablePermits());

    as.release(1);
    Assert.assertEquals(1, as.getAvailablePermits());
    Assert.assertEquals(1, as.drainPermits());
    Assert.assertEquals(0, as.getAvailablePermits());
  }

  @Test
  public void testStackUnroll() {
    final AsyncSemaphore as = createSemaphore(0);

    for (int i = 0; i < 50_000; i++) {
      as.acquire().whenComplete((t, e) -> as.release());
    }

    as.release();
    Assert.assertEquals(1, as.getAvailablePermits());
  }

  @Test
  public void testGetQueueLength() {
    for (final long initPermits : new long[] {0, 1}) {
      final AsyncSemaphore as = createSemaphore(initPermits);
      Assert.assertEquals(0, as.getQueueLength());
    }

    final AsyncSemaphore as = createSemaphore(10);
    as.acquire(5); // 5 avail
    Assert.assertEquals(0, as.getQueueLength());
    as.tryAcquire(3); // 2 avail
    Assert.assertEquals(0, as.getQueueLength());
    as.acquire(3); // -1, a waiter
    Assert.assertEquals(1, as.getQueueLength());
    for (int i = 2; i <= 20; i++) {
      as.acquire();
      Assert.assertEquals(i, as.getQueueLength());
    }
    Assert.assertEquals(20, as.getQueueLength());
    as.release();
    Assert.assertEquals(19, as.getQueueLength());
    for (int i = 19; i > 1; i -= 2) {
      Assert.assertEquals(i, as.getQueueLength());
      as.release(2);
    }
    Assert.assertEquals(1, as.getQueueLength());
    as.acquire();
    Assert.assertEquals(2, as.getQueueLength());
    as.release();
    Assert.assertEquals(1, as.getQueueLength());
    as.release(20);
    Assert.assertEquals(0, as.getQueueLength());
    as.acquire();
    Assert.assertEquals(0, as.getQueueLength());
  }

  public static abstract class AbstractAsyncSemaphoreFairnessTest
      extends AbstractAsyncSemaphoreTest {
    /*
     * includes testing specific to the fair acquisition policy
     */

    AbstractAsyncSemaphoreFairnessTest(final long maxPermits) {
      super(maxPermits);
    }

    @Test
    public void testQueueAndNoBarging() throws Exception {
      final AsyncSemaphore as = createSemaphore(0);

      final CompletableFuture<Void> f0 = as.acquire(5).toCompletableFuture();
      final CompletableFuture<Void> f1 = as.acquire(3).toCompletableFuture();
      final CompletableFuture<Void> f2 = as.acquire(1).toCompletableFuture();

      Assert.assertFalse(f0.isDone());
      Assert.assertFalse(f1.isDone());
      Assert.assertFalse(f2.isDone());

      as.release(1);

      // maintain insertion queue, despite available permit
      Assert.assertFalse(f0.isDone());
      Assert.assertFalse(f1.isDone());
      Assert.assertFalse(f2.isDone());

      // no barging, despite available permit
      final CompletableFuture<Void> f3 = as.acquire(1).toCompletableFuture();
      Assert.assertFalse(f3.isDone());

      as.release(3);

      Assert.assertFalse(f0.isDone());
      Assert.assertFalse(f1.isDone());
      Assert.assertFalse(f2.isDone());
      Assert.assertFalse(f3.isDone());

      as.release(1);

      TestUtil.join(f0, 1, TimeUnit.SECONDS);
      Assert.assertFalse(f1.isDone());
      Assert.assertFalse(f2.isDone());
      Assert.assertFalse(f3.isDone());

      as.release(2);

      Assert.assertFalse(f1.isDone());
      Assert.assertFalse(f2.isDone());
      Assert.assertFalse(f3.isDone());

      final CompletableFuture<Void> f4 = as.acquire(1).toCompletableFuture();
      Assert.assertFalse(f4.isDone());

      as.release(1);

      TestUtil.join(f1, 1, TimeUnit.SECONDS);
      Assert.assertFalse(f2.isDone());
      Assert.assertFalse(f3.isDone());
      Assert.assertFalse(f4.isDone());

      as.release(1);
      TestUtil.join(f2, 1, TimeUnit.SECONDS);
      Assert.assertFalse(f3.isDone());
      Assert.assertFalse(f4.isDone());

      as.release(1);
      TestUtil.join(f3, 1, TimeUnit.SECONDS);
      Assert.assertFalse(f4.isDone());

      as.release(1);
      TestUtil.join(f4, 1, TimeUnit.SECONDS);

      Assert.assertEquals(0, as.getAvailablePermits());
    }

    @Test
    public void testAcquireZeroOnConstructor() throws Exception {
      {
        final AsyncSemaphore as = createSemaphore(0);
        Assert.assertEquals(0, as.getAvailablePermits());
        TestUtil.join(as.acquire(0), 1, TimeUnit.SECONDS);
        Assert.assertEquals(0, as.getAvailablePermits());
      }

      {
        final AsyncSemaphore as = createSemaphore(1);
        Assert.assertEquals(1, as.getAvailablePermits());
        TestUtil.join(as.acquire(0), 1, TimeUnit.SECONDS);
        Assert.assertEquals(1, as.getAvailablePermits());
      }

      {
        final AsyncSemaphore as = createSemaphore(-1);
        Assert.assertEquals(-1, as.getAvailablePermits());
        final CompletableFuture<Void> f = as.acquire(0).toCompletableFuture();
        Assert.assertFalse(f.isDone());
        as.release(1);
        TestUtil.join(as.acquire(0), 1, TimeUnit.SECONDS);
        Assert.assertEquals(0, as.getAvailablePermits());
      }
    }

    @Test
    public void testAcquireZero() throws Exception {
      final AsyncSemaphore as = createSemaphore(0);

      {
        Assert.assertEquals(0, as.getAvailablePermits());
        TestUtil.join(as.acquire(0), 1, TimeUnit.SECONDS);
        Assert.assertEquals(0, as.getAvailablePermits());
      }

      {
        as.release(1);
        Assert.assertEquals(1, as.getAvailablePermits());
        TestUtil.join(as.acquire(0), 1, TimeUnit.SECONDS);
        Assert.assertEquals(1, as.getAvailablePermits());
      }

      {
        final CompletableFuture<Void> blockingAcq = as.acquire(3).toCompletableFuture();
        Assert.assertFalse(blockingAcq.isDone());
        final CompletableFuture<Void> zeroAcq = as.acquire(0).toCompletableFuture();
        Assert.assertFalse(zeroAcq.isDone());
        as.release(1);

        Assert.assertFalse(blockingAcq.isDone());
        Assert.assertFalse(zeroAcq.isDone());
        final CompletableFuture<Void> blockingAcq2 = as.acquire(1).toCompletableFuture();
        final CompletableFuture<Void> zeroAcq2 = as.acquire(0).toCompletableFuture();
        Assert.assertFalse(blockingAcq2.isDone());
        Assert.assertFalse(zeroAcq2.isDone());

        as.release(1);
        TestUtil.join(blockingAcq, 1, TimeUnit.SECONDS);
        TestUtil.join(zeroAcq, 1, TimeUnit.SECONDS);
        Assert.assertFalse(blockingAcq2.isDone());
        Assert.assertFalse(zeroAcq2.isDone());

        as.release(1);
        TestUtil.join(blockingAcq2, 1, TimeUnit.SECONDS);
        TestUtil.join(zeroAcq2, 1, TimeUnit.SECONDS);
      }
    }

    @Test
    public void testTryAcquireZeroOnConstructor() throws Exception {
      {
        final AsyncSemaphore as = createSemaphore(1);
        Assert.assertTrue(as.tryAcquire(0));
        as.acquire(1);
        Assert.assertTrue(as.tryAcquire(0));
        as.acquire(1);
        Assert.assertFalse(as.tryAcquire(0));
      }

      {
        final AsyncSemaphore as = createSemaphore(0);
        Assert.assertTrue(as.tryAcquire(0));
        as.acquire(1);
        Assert.assertFalse(as.tryAcquire(0));
        as.release(1);
        Assert.assertTrue(as.tryAcquire(0));
      }

      {
        final AsyncSemaphore as = createSemaphore(-1);
        Assert.assertFalse(as.tryAcquire(0));
        as.release(1);
        Assert.assertTrue(as.tryAcquire(0));
      }
    }

    @Test
    public void testTryAcquireZero() throws Exception {
      final AsyncSemaphore as = createSemaphore(1);

      Assert.assertTrue(as.tryAcquire(0));
      TestUtil.join(as.acquire(1), 1, TimeUnit.SECONDS);

      Assert.assertTrue(as.tryAcquire(0));
      as.acquire(1);
      Assert.assertFalse(as.tryAcquire(0));
      as.release(1);
      Assert.assertTrue(as.tryAcquire(0));
    }
  }

  public static class SyncFairAsyncSemaphoreTest extends AbstractAsyncSemaphoreFairnessTest {
    @Override
    protected boolean isActuallyAsync() {
      return false;
    }

    @Override
    protected boolean supportsLongArgs() {
      return false;
    }

    private static ExecutorService pool;

    public SyncFairAsyncSemaphoreTest() {
      super(Integer.MAX_VALUE);
    }

    // ignore queue length test because the asynchrony with executors is hard to test without races
    @Override
    public void testGetQueueLength() {}


    // ignore tryAcquire(0) because barging makes it meaningless to test
    @Override
    public void testTryAcquireZero() throws Exception {}

    @Override
    public void testTryAcquireZeroOnConstructor() throws Exception {}

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
    protected AsyncSemaphore createSemaphore(final long initialPermits) {
      return new SyncAsyncSemaphore(initialPermits, true, pool);
    }
  }
}


class SemaphoreAsAsyncReadWriteLock implements AsyncReadWriteLock {
  private final AsyncSemaphore semaphore;
  private final long maxPermits;

  public SemaphoreAsAsyncReadWriteLock(final LongFunction<AsyncSemaphore> semaphoreFactory,
      final long maxPermits) {
    this.maxPermits = maxPermits;
    this.semaphore = semaphoreFactory.apply(maxPermits);
  }

  @Override
  public CompletionStage<ReadLockToken> acquireReadLock() {
    return this.semaphore.acquire(1)
        .thenApply(ignored -> new RunOnceToken(1)::release);
  }

  @Override
  public Optional<ReadLockToken> tryReadLock() {
    return this.semaphore.tryAcquire(1)
        ? Optional.of(new RunOnceToken(1)::release)
        : Optional.empty();
  }

  @Override
  public CompletionStage<WriteLockToken> acquireWriteLock() {
    return this.semaphore.acquire(this.maxPermits)
        .thenApply(ignored -> new RunOnceWriteToken(this.maxPermits));
  }

  @Override
  public Optional<WriteLockToken> tryWriteLock() {
    return this.semaphore.tryAcquire(this.maxPermits)
        ? Optional.of(new RunOnceWriteToken(this.maxPermits))
        : Optional.empty();
  }

  private class RunOnceToken {
    private boolean hasRun = false;
    final long permits;

    RunOnceToken(final long permits) {
      this.permits = permits;
    }

    void release() {
      release(this.permits);
    }

    void release(final long n) {
      if (this.hasRun) {
        throw new IllegalStateException("released lock more than once");
      }
      SemaphoreAsAsyncReadWriteLock.this.semaphore.release(n);
      this.hasRun = true;
    }
  }

  private class RunOnceWriteToken extends RunOnceToken implements WriteLockToken {

    RunOnceWriteToken(final long permits) {
      super(permits);
    }

    @Override
    public void releaseWriteLock() {
      release();
    }

    @Override
    public ReadLockToken downgradeLock() {
      assert this.permits > 1;
      release(this.permits - 1);
      return new RunOnceToken(1)::release;
    }
  }
}
