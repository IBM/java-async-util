package com.ibm.async_util.iteration;

import com.ibm.async_util.util.Either;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;

public class AsyncIteratorCloseTest {
  private static class TestException extends RuntimeException {};

  private static RuntimeException testException = new TestException();

  static class CloseableIterator implements AsyncIterator<Integer> {
    private AsyncIterator<Integer> backing;
    private RuntimeException ex;
    boolean closed;

    CloseableIterator(AsyncIterator<Integer> backing) {
      this(backing, null);
    }

    CloseableIterator(AsyncIterator<Integer> backing, final RuntimeException ex) {
      this.backing = backing;
      this.ex = ex;
      this.closed = false;
    }

    @Override
    public CompletionStage<Either<End, Integer>> nextFuture() {
      return backing.nextFuture();
    }

    @Override
    public CompletionStage<Void> close() {
      return backing
          .close()
          .whenComplete(
              (ig, e) -> {
                this.closed = true;
                if (ex != null) {
                  throw ex;
                }
              });
    }
  }

  @Test
  public void testConcatNoConsumptionClose() {
    // don't finish any of the concatted iterators
    CloseableIterator it1 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    CloseableIterator it2 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    CloseableIterator it3 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    AsyncIterator<Integer> concat = AsyncIterator.concat(Arrays.asList(it1, it2, it3).iterator());
    concat.nextFuture().toCompletableFuture().join();
    Assert.assertFalse(it1.closed || it2.closed || it3.closed);
    concat.close().toCompletableFuture().join();
    Assert.assertTrue(it1.closed);
    Assert.assertFalse(it2.closed || it3.closed);
  }

  @Test
  public void testConcatClose() {
    CloseableIterator it1 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    CloseableIterator it2 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    CloseableIterator it3 = new CloseableIterator(AsyncIterator.range(0, 3, 1));
    AsyncIterator<Integer> concat = AsyncIterator.concat(Arrays.asList(it1, it2, it3).iterator());

    concat.consume().toCompletableFuture().join();

    // fully consumed iterators must be closed
    Assert.assertTrue(it1.closed && it2.closed);

    concat.close().toCompletableFuture().join();

    // all iterators should be closed now
    Assert.assertTrue(it3.closed);
  }

  @Test
  public void testConcatCloseException() {
    CloseableIterator it1 = new CloseableIterator(AsyncIterator.once(1));
    // close throws an exception
    CloseableIterator it2 = new CloseableIterator(AsyncIterator.once(2), testException);
    CloseableIterator it3 = new CloseableIterator(AsyncIterator.once(3));
    AsyncIterator<Integer> concat = AsyncIterator.concat(Arrays.asList(it1, it2, it3).iterator());

    Assert.assertEquals(
        1, concat.nextFuture().toCompletableFuture().join().right().get().intValue());
    Assert.assertEquals(
        2, concat.nextFuture().toCompletableFuture().join().right().get().intValue());
    try {
      concat.nextFuture().toCompletableFuture().join();
      Assert.fail("exception expected");
    } catch (CompletionException e) {
    }
    // we should be able to ignore the close exception
    Assert.assertEquals(
        3, concat.nextFuture().toCompletableFuture().join().right().get().intValue());
    concat.close().toCompletableFuture().join();
    Assert.assertTrue(it1.closed && it2.closed && it3.closed);
  }

  @Test
  public void testFlattenClose() {
    AsyncIterator<Integer> it = AsyncIterator.range(0, 3, 1);
    List<CloseableIterator> closeables = new ArrayList<>();
    AsyncIterator<Integer> flattend =
        it.thenFlatten(
            i -> {
              CloseableIterator ret = new CloseableIterator(AsyncIterator.range(0, 3, 1));
              closeables.add(ret);
              return ret;
            });

    for (int i = 0; i < 4; i++) flattend.nextFuture().toCompletableFuture().join();
    Assert.assertTrue(closeables.get(0).closed);
    for (int i = 0; i < 3; i++) flattend.nextFuture().toCompletableFuture().join();
    Assert.assertTrue(closeables.get(1).closed);

    flattend.close().toCompletableFuture().join();

    // fully consumed iterators must be closed
    Assert.assertTrue(closeables.get(2).closed);
  }

  @Test
  public void testZipWithClose() {
      for (boolean it1Failed : new Boolean[]{false, true}) {
        for (boolean it2Failed : new Boolean[]{false, true}) {
          CloseableIterator it1 = new CloseableIterator(AsyncIterator.range(0, 3, 1), it1Failed ? testException : null);
          CloseableIterator it2 = new CloseableIterator(AsyncIterator.range(0, 3, 1), it2Failed ? testException : null);
          AsyncIterator<Integer> zipped = AsyncIterator.zipWith(it1, it2, (i, j) -> i + j);
          zipped.nextFuture().toCompletableFuture().join();
          boolean expectFailure = it1Failed || it2Failed;
          try {
            zipped.close().toCompletableFuture().join();
            Assert.assertFalse("expected exception", expectFailure);
          } catch (CompletionException e) {
            Assert.assertTrue("unexpected exception " + e, expectFailure);
          }
          // both should be closed no matter what
          Assert.assertTrue(it1.closed && it2.closed);
        }
      }
  }

  @Test
  public void testEagerClose() throws InterruptedException {
    AsyncIterator<Integer> it = AsyncIterator.range(0, 15, 1);
    Deque<CloseableIterator> closeables = new ConcurrentLinkedDeque<CloseableIterator>();
    final CountDownLatch closeablesGenerated = new CountDownLatch(1);
    AsyncIterator<Integer> ahead =
        it.thenFlattenAhead(
            i -> {
              CloseableIterator closeable = new CloseableIterator(AsyncIterator.range(0, 3, 1));
              closeables.addLast(closeable);
              closeablesGenerated.countDown();
              return closeable;
            },
            5);
    CompletionStage<Either<AsyncIterator.End, Integer>> first = ahead.nextFuture();
    closeablesGenerated.await();
    // 6: 5 eagerly evaluated items, + 1 we evaluated
    Assert.assertEquals(6, closeables.size());
    Assert.assertEquals(0, first.toCompletableFuture().join().right().get().intValue());

    // if we consume the first iterator we generated (3 elements) it should be closed
    for (int i = 0; i < 3; i++) ahead.nextFuture().toCompletableFuture().join();
    Assert.assertTrue(closeables.getFirst().closed);

    // close and make sure eagerly evaluated iterators are closed
    ahead.close().toCompletableFuture().join();
    Assert.assertTrue(closeables.stream().allMatch(closeableIterator -> closeableIterator.closed));
  }
}
