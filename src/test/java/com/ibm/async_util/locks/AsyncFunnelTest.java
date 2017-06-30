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
// @author: rkhadiwala
//
// Date: Aug 4, 2015
// ---------------------

package com.ibm.async_util.locks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.ibm.async_util.iteration.AsyncFunnel;
import org.junit.Assert;
import org.junit.Test;

public class AsyncFunnelTest {

  private static class TestException extends Exception {
    private static final long serialVersionUID = 1L;
    private final int i;

    public TestException(final int i) {
      this.i = i;

    }

    public int get() {
      return this.i;
    }
  }


  @Test
  public void testNullify() {
    final AsyncFunnel<Integer> c = new AsyncFunnel<>();
    for (int k = 0; k < 5; k++) {
      final int finalK = k;
      CompletionStage<Integer> second = c.doOrGet(() -> {
        CompletableFuture<Integer> result = new CompletableFuture<>();
        if (finalK % 2 == 0) {
          result.complete(finalK);
        } else {
          result.completeExceptionally(new TestException(finalK));
        }
        return result;
      });
      int j;
      try {
        j = second.toCompletableFuture().join();
      } catch (final CompletionException e) {
        j = ((TestException) e.getCause()).get();
      }
      Assert.assertEquals(j, k);
    }
  }

  @Test
  public void funnelTest() throws InterruptedException, ExecutionException, TimeoutException {
    final AsyncFunnel<Integer> c = new AsyncFunnel<>();
    final CountDownLatch latch1 = new CountDownLatch(1);
    c.doOrGet(() -> CompletableFuture.supplyAsync(() -> {
      try {
        latch1.await();
      } catch (InterruptedException e) {
      }
      return 1;
    }));

    // this should not be called
    CompletableFuture<Integer> fail =
        c.doOrGet(() -> CompletableFuture.completedFuture(-1)).toCompletableFuture();
    Assert.assertFalse(fail.isDone());
    
    // let the first future finish
    latch1.countDown();
    Assert.assertEquals(1, fail.get(1, TimeUnit.SECONDS).intValue());

    // this can be accepted immediately
    CompletableFuture<Integer> second =
        c.doOrGet(() -> CompletableFuture.completedFuture(2)).toCompletableFuture();
    Assert.assertTrue(second.isDone());
    Assert.assertEquals(2, second.join().intValue());
    
    // so can this
    CompletableFuture<Integer> third =
        c.doOrGet(() -> {
          CompletableFuture<Integer> x = new CompletableFuture<>();
          x.completeExceptionally(new Exception());
          return x;
        }).toCompletableFuture();
    Assert.assertTrue(third.isDone());
    Assert.assertTrue(third.isCompletedExceptionally());
  }

  @Test
  public void concurrentFunnelTest() throws InterruptedException, ExecutionException, TimeoutException {
    final int NUM_THREADS = 3;
    final AsyncFunnel<Integer> c = new AsyncFunnel<>();
    final AtomicInteger count = new AtomicInteger(0);
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    final CountDownLatch secondSubmitted = new CountDownLatch(1);
    final AtomicBoolean running = new AtomicBoolean(true);
    c.doOrGet(() -> CompletableFuture.supplyAsync(() -> {
      int next = count.incrementAndGet();
      try {
        latch1.await();
      } catch (InterruptedException e) {
      }
      return next;
    }));
    @SuppressWarnings("rawtypes")
    CompletableFuture[] futures = IntStream.range(0, NUM_THREADS).mapToObj(i -> {
      return CompletableFuture.runAsync(() -> {
        while (running.get()) {
          c.doOrGet(() -> CompletableFuture.supplyAsync(() -> {
            secondSubmitted.countDown();
            int next = count.incrementAndGet();
            try {
              latch2.await();
            } catch (InterruptedException e) {
            }
            return next;
          }));
        }
      });
    }).toArray(CompletableFuture[]::new);
    
    Assert.assertEquals(count.get(), 1);
    CompletableFuture<Integer> first =
        c.doOrGet(() -> CompletableFuture.completedFuture(-1)).toCompletableFuture();
    Assert.assertFalse(first.isDone());
    Assert.assertEquals(1, secondSubmitted.getCount());
    latch1.countDown();
    Assert.assertEquals(1, first.get(1, TimeUnit.SECONDS).intValue());

    // let any of the threads get the next future in
    Assert.assertTrue(secondSubmitted.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(count.get(), 2);
    CompletableFuture<Integer> second =
        c.doOrGet(() -> CompletableFuture.completedFuture(-1)).toCompletableFuture();
    Assert.assertFalse(second.isDone());
    latch2.countDown();
    Assert.assertEquals(2, second.get(1, TimeUnit.SECONDS).intValue());

    running.set(false);
    CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);
  }
}
