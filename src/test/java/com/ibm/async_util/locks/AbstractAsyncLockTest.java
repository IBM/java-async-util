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
// Date: Dec 26, 2016
// ---------------------

package com.ibm.async_util.locks;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.ibm.async_util.util.Reference;
import com.ibm.async_util.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class AbstractAsyncLockTest {

  protected abstract AsyncLock getLock();

  /**
   * Stack overflow tests tend to fail (or require huge thread growth) when using sanity stdlib sync
   * backed tests
   */
  protected boolean isActuallyAsync() {
    return true;
  }

  @Test
  public final void testMutex() {
    final AsyncLock lock = getLock();

    final AtomicInteger participants = new AtomicInteger();
    final Function<AsyncLock.LockToken, AsyncLock.LockToken> mutexCheck = input -> {
      if (participants.getAndIncrement() != 0) {
        throw new AssertionError("lock not mutually exclusive");
      }
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (participants.decrementAndGet() != 0) {
        throw new AssertionError("lock not mutually exclusive");
      }
      return input;
    };

    final int parallel = 20;
    final List<CompletionStage<AsyncLock.LockToken>> acqs = IntStream.range(0, parallel)
        // start acquisitions in separate threads, but collect the futures back together whether
        // they're done or not
        .mapToObj(
            i -> CompletableFuture.supplyAsync(() -> lock.acquireLock().thenApply(mutexCheck)))
        .map(CompletableFuture::join).collect(Collectors.toList());

    TestUtil.join(CompletableFuture.allOf(acqs.stream()
        .map(f -> f.thenAccept(AsyncLock.LockToken::releaseLock)).toArray(CompletableFuture[]::new)));

  }

  @Test
  public final void testStackOverflow() throws Exception {
    Assume.assumeTrue(isActuallyAsync());

    // test no overflow
    {
      final AsyncLock lock = getLock();
      final CompletionStage<AsyncLock.LockToken> firstLock = lock.acquireLock();
      final Reference<CompletionStage<AsyncLock.LockToken>> lastLock = new Reference<>(null);

      CompletableFuture.runAsync(() -> {
        // chain futures on the single write lock
        for (int i = 0; i < 100_000; i++) {
          lock.acquireLock().thenAccept(AsyncLock.LockToken::releaseLock);
        }
        lastLock.set(lock.acquireLock().thenApply(token -> {
          token.releaseLock();
          return token;
        }));
      }).join();

      TestUtil.join(firstLock).releaseLock();
      TestUtil.join(lastLock.get(), 2, TimeUnit.SECONDS);
    }

    // test threading ABA for unroll
    {
      final Executor execA = Executors.newSingleThreadExecutor();
      final Executor execB = Executors.newSingleThreadExecutor();

      final AsyncLock lock = getLock();
      final AsyncLock.LockToken acq1 = TestUtil.join(lock.acquireLock());

      // acquires primed to release on threads: main, A, B, A
      final CompletableFuture<Void> acq2 =
          lock.acquireLock().thenAcceptAsync(AsyncLock.LockToken::releaseLock, execA).toCompletableFuture();
      final CompletableFuture<Void> acq3 =
          lock.acquireLock().thenAcceptAsync(AsyncLock.LockToken::releaseLock, execB).toCompletableFuture();
      final CompletableFuture<Void> acq4 =
          lock.acquireLock().thenAcceptAsync(AsyncLock.LockToken::releaseLock, execA).toCompletableFuture();


      acq1.releaseLock();
      // wait for everybody to do everything
      TestUtil.join(CompletableFuture.allOf(acq2, acq3, acq4));
      TestUtil.join(lock.acquireLock().thenAccept(AsyncLock.LockToken::releaseLock), 2, TimeUnit.SECONDS);
    }

    // test threading AA for unroll (release on same thread, but separate stacks)
    {
      final Executor execA = Executors.newSingleThreadExecutor();

      final AsyncLock lock = getLock();
      final AsyncLock.LockToken acq1 = TestUtil.join(lock.acquireLock());

      // acqrs primed to release on threads: main, A, A
      final CompletableFuture<Void> acq2 =
          lock.acquireLock().thenAcceptAsync(AsyncLock.LockToken::releaseLock, execA).toCompletableFuture();
      final CompletableFuture<Void> acq3 =
          lock.acquireLock().thenAcceptAsync(AsyncLock.LockToken::releaseLock, execA).toCompletableFuture();


      acq1.releaseLock();
      TestUtil.join(CompletableFuture.allOf(acq2, acq3));
      TestUtil.join(lock.acquireLock().thenAccept(AsyncLock.LockToken::releaseLock), 2, TimeUnit.SECONDS);
    }
  }

  @SuppressWarnings("serial")
  private static class LockStateException extends RuntimeException {
  }

  @Test(expected = LockStateException.class)
  public final void testReleaseException() {
    final AsyncLock.LockToken token = TestUtil.join(getLock().acquireLock());

    token.releaseLock();
    try {
      token.releaseLock();
    } catch (IllegalStateException | IllegalMonitorStateException expected) {
      throw new LockStateException();
    } catch (final Error e) {
      // j.u.c.Semaphore throws an Error with this message
      if (e.getMessage().equals("Maximum permit count exceeded")) {
        throw new LockStateException();
      } else {
        throw e;
      }
    }
  }

  @Test
  public final void testTryLock() throws TimeoutException {
    final AsyncLock lock = getLock();
    // first lock should succeed
    final AsyncLock.LockToken acq1 = lock.tryLock().orElseThrow(AssertionError::new);
    // other lock should fail
    Assert.assertFalse(lock.tryLock().isPresent());

    final CompletableFuture<AsyncLock.LockToken> acq2 = lock.acquireLock().toCompletableFuture();
    Assert.assertFalse(acq2.isDone());

    acq1.releaseLock();
    final AsyncLock.LockToken acq2Token = TestUtil.join(acq2, 500, TimeUnit.MILLISECONDS);

    Assert.assertFalse(lock.tryLock().isPresent());
    acq2Token.releaseLock();

    lock.tryLock().orElseThrow(AssertionError::new).releaseLock();
  }

  public static abstract class AbstractAsyncLockFairnessTest extends AbstractAsyncLockTest {
    @Test
    public final void testFairness() throws Exception {
      final AsyncLock lock = getLock();

      final CompletableFuture<AsyncLock.LockToken> acq1 = lock.acquireLock().toCompletableFuture();
      final CompletableFuture<AsyncLock.LockToken> acq2 = lock.acquireLock().toCompletableFuture();
      final CompletableFuture<AsyncLock.LockToken> acq3 = lock.acquireLock().toCompletableFuture();

      final AsyncLock.LockToken token1 = TestUtil.join(acq1, 500, TimeUnit.MILLISECONDS);
      Assert.assertFalse(acq2.isDone());
      Assert.assertFalse(acq3.isDone());

      token1.releaseLock();

      final CompletableFuture<AsyncLock.LockToken> acq4 = lock.acquireLock().toCompletableFuture();
      final AsyncLock.LockToken token2 = TestUtil.join(acq2, 500, TimeUnit.MILLISECONDS);
      Assert.assertFalse(acq3.isDone());
      Assert.assertFalse(acq4.isDone());

      token2.releaseLock();

      final AsyncLock.LockToken token3 = TestUtil.join(acq3, 500, TimeUnit.MILLISECONDS);
      Assert.assertFalse(acq4.isDone());

      token3.releaseLock();

      TestUtil.join(acq4, 500, TimeUnit.MILLISECONDS).releaseLock();
    }
  }

  public static class SyncSemaphoreAsyncLockTest extends AbstractAsyncLockFairnessTest {
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
    protected AsyncLock getLock() {
      return new RWLockAsAsyncLock(new SemaphoreAsAsyncReadWriteLock(
          permits -> new SyncAsyncSemaphore(permits, true, pool), Integer.MAX_VALUE));
    }
  }
}

