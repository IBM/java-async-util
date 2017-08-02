package com.ibm.async_util.iteration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.async_util.util.Either;
import com.ibm.async_util.util.StageSupport;
import com.ibm.async_util.util.TestUtil;

public class AsyncIteratorCloseTest {
  private static class TestException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  private static final RuntimeException testException = new TestException();

  static class CloseableIterator implements AsyncIterator<Integer> {
    private final AsyncIterator<Integer> backing;
    private final RuntimeException ex;
    boolean closed;

    CloseableIterator(final AsyncIterator<Integer> backing) {
      this(backing, null);
    }

    CloseableIterator(final AsyncIterator<Integer> backing, final RuntimeException ex) {
      this.backing = backing;
      this.ex = ex;
      this.closed = false;
    }

    @Override
    public CompletionStage<Either<End, Integer>> nextFuture() {
      return this.backing.nextFuture();
    }

    @Override
    public CompletionStage<Void> close() {
      return this.backing
          .close()
          .whenComplete(
              (ig, e) -> {
                this.closed = true;
                if (this.ex != null) {
                  throw this.ex;
                }
              });
    }
  }

  @Test
  public void testConcatNoConsumptionClose() {
    // don't finish any of the concatted iterators
    final CloseableIterator it1 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    final CloseableIterator it2 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    final CloseableIterator it3 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    final AsyncIterator<Integer> concat =
        AsyncIterator.concat(Arrays.asList(it1, it2, it3).iterator());
    concat.nextFuture().toCompletableFuture().join();
    Assert.assertFalse(it1.closed || it2.closed || it3.closed);
    concat.close().toCompletableFuture().join();
    Assert.assertTrue(it1.closed);
    Assert.assertFalse(it2.closed || it3.closed);
  }

  @Test
  public void testConcatClose() {
    final CloseableIterator it1 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    final CloseableIterator it2 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    final CloseableIterator it3 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    final AsyncIterator<Integer> concat =
        AsyncIterator.concat(Arrays.asList(it1, it2, it3).iterator());

    concat.consume().toCompletableFuture().join();

    // fully consumed iterators must be closed
    Assert.assertTrue(it1.closed && it2.closed);

    concat.close().toCompletableFuture().join();

    // all iterators should be closed now
    Assert.assertTrue(it3.closed);
  }

  @Test
  public void testConcatCloseException() {
    final CloseableIterator it1 = new CloseableIterator(AsyncIterator.once(1));
    // close throws an exception
    final CloseableIterator it2 = new CloseableIterator(AsyncIterator.once(2), testException);
    final CloseableIterator it3 = new CloseableIterator(AsyncIterator.once(3));
    final AsyncIterator<Integer> concat =
        AsyncIterator.concat(Arrays.asList(it1, it2, it3).iterator());

    Assert.assertEquals(
        1, concat.nextFuture().toCompletableFuture().join().right().get().intValue());
    Assert.assertEquals(
        2, concat.nextFuture().toCompletableFuture().join().right().get().intValue());
    try {
      concat.nextFuture().toCompletableFuture().join();
      Assert.fail("exception expected");
    } catch (final CompletionException e) {
    }
    // we should be able to ignore the close exception
    Assert.assertEquals(
        3, concat.nextFuture().toCompletableFuture().join().right().get().intValue());
    concat.close().toCompletableFuture().join();
    Assert.assertTrue(it1.closed && it2.closed && it3.closed);
  }

  @Test
  public void testFlattenClose() {
    final AsyncIterator<Integer> it = AsyncIterator.range(0, 3, 1);
    final List<CloseableIterator> closeables = new ArrayList<>();
    final AsyncIterator<Integer> flattend =
        it.thenFlatten(
            i -> {
              final CloseableIterator ret = new CloseableIterator(AsyncIterator.range(0, 3, 1));
              closeables.add(ret);
              return ret;
            });

    for (int i = 0; i < 4; i++) {
      flattend.nextFuture().toCompletableFuture().join();
    }
    Assert.assertTrue(closeables.get(0).closed);
    for (int i = 0; i < 3; i++) {
      flattend.nextFuture().toCompletableFuture().join();
    }
    Assert.assertTrue(closeables.get(1).closed);

    flattend.close().toCompletableFuture().join();

    // fully consumed iterators must be closed
    Assert.assertTrue(closeables.get(2).closed);
  }

  @Test
  public void testZipWithClose() {
    for (final boolean it1Failed : new Boolean[] {false, true}) {
      for (final boolean it2Failed : new Boolean[] {false, true}) {
        final CloseableIterator it1 =
            new CloseableIterator(AsyncIterator.range(0, 3, 1), it1Failed ? testException : null);
        final CloseableIterator it2 =
            new CloseableIterator(AsyncIterator.range(0, 3, 1), it2Failed ? testException : null);
        final AsyncIterator<Integer> zipped = AsyncIterator.zipWith(it1, it2, (i, j) -> i + j);
        zipped.nextFuture().toCompletableFuture().join();
        final boolean expectFailure = it1Failed || it2Failed;
        try {
          zipped.close().toCompletableFuture().join();
          Assert.assertFalse("expected exception", expectFailure);
        } catch (final CompletionException e) {
          Assert.assertTrue("unexpected exception " + e, expectFailure);
        }
        // both should be closed no matter what
        Assert.assertTrue(it1.closed && it2.closed);
      }
    }
  }

  @Test
  public void testEagerFlattenClose() throws InterruptedException {
    final AsyncIterator<Integer> it = AsyncIterator.range(0, 15, 1);
    final Deque<CloseableIterator> closeables = new ConcurrentLinkedDeque<CloseableIterator>();
    final CountDownLatch closeablesGenerated = new CountDownLatch(1);
    final AsyncIterator<Integer> ahead =
        it.thenFlattenAhead(
            i -> {
              final CloseableIterator closeable =
                  new CloseableIterator(AsyncIterator.range(0, 3, 1));
              closeables.addLast(closeable);
              closeablesGenerated.countDown();
              return StageSupport.completedStage(closeable);
            },
            5);
    final CompletionStage<Either<AsyncIterator.End, Integer>> first = ahead.nextFuture();
    closeablesGenerated.await();
    // 6: 5 eagerly evaluated items, + 1 we evaluated
    Assert.assertEquals(6, closeables.size());
    Assert.assertEquals(0, first.toCompletableFuture().join().right().get().intValue());

    // if we consume the first iterator we generated (3 elements) it should be closed
    for (int i = 0; i < 3; i++) {
      ahead.nextFuture().toCompletableFuture().join();
    }
    Assert.assertTrue(closeables.getFirst().closed);

    // close and make sure eagerly evaluated iterators are closed
    ahead.close().toCompletableFuture().join();
    Assert.assertTrue(closeables.stream().allMatch(closeableIterator -> closeableIterator.closed));
  }


  @Test
  public void testEagerComposeClose() throws InterruptedException {
    final AsyncIterator<Integer> it = AsyncIterator.range(0, 15, 1);

    final CountDownLatch latch = new CountDownLatch(1);
    final Collection<Integer> objects = new ArrayList<>();
    final AsyncIterator<Integer> ahead =
        it.thenComposeAhead(i -> {
          latch.countDown();
          objects.add(1);
          return StageSupport.completedStage(objects.size());
        }, 5);

    final CompletionStage<Either<AsyncIterator.End, Integer>> first = ahead.nextFuture();
    latch.await();
    // 6: 5 eagerly evaluated items, + 1 we evaluated
    Assert.assertEquals(6, objects.size());
    Assert.assertEquals(1, first.toCompletableFuture().join().right().get().intValue());

    // close
    ahead.close().toCompletableFuture().join();
  }

  @Test(expected = IllegalStateException.class)
  public void testNextFutureAfterCloseIllegal() throws Throwable {
    final AsyncIterator<Integer> it = AsyncIterator.range(0, 15, 1);
    final AsyncIterator<Integer> ahead =
        it.thenComposeAhead(i -> StageSupport.completedStage(i + 1), 5);

    final CompletionStage<Either<AsyncIterator.End, Integer>> first = ahead.nextFuture();

    TestUtil.join(first);
    TestUtil.join(ahead.close());
    try {
      TestUtil.join(ahead.nextFuture());
    } catch (final CompletionException e) {
      throw e.getCause();
    }
  }

}
