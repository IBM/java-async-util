/* 
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.asyncutil.iteration.AsyncIterator;
import com.ibm.asyncutil.util.Either;
import com.ibm.asyncutil.util.StageSupport;

/**
 * For AsyncIterator specific tests that TCK may not cover
 */
public class FlowAdapterTest {

  @Test(expected = IOException.class)
  public void testJustError() throws Throwable {
    FlowAdapterTest.consume(new ExceptionalIterator(5, new IOException(), null));
  }

  @Test(expected = IOException.class)
  public void testJustCloseError() throws Throwable {
    FlowAdapterTest.consume(new ExceptionalIterator(5, null, new IOException()));
  }

  @Test
  public void testCloseErrorSuppressed() throws Throwable {
    try {
      FlowAdapterTest
          .consume(new ExceptionalIterator(5, new IOException(), new TimeoutException()));
      Assert.fail("iterator should have thrown exception");
    } catch (final IOException e) {
      final Throwable[] arr = e.getSuppressed();
      Assert.assertTrue(arr.length == 1);
      Assert.assertTrue(arr[0] instanceof TimeoutException);
    }
  }

  @Test(expected = IOException.class)
  public void testCancelCloseException() throws Throwable {
    final ConsumingSubscriber<Long> subscriber = new ConsumingSubscriber<Long>() {
      @Override
      public void onNext(final Long arg0) {}
    };
    final Publisher<Long> publisher =
        FlowAdapter.toPublisher(new ExceptionalIterator(10, null, new IOException()));
    publisher.subscribe(subscriber);
    subscriber.request();
    subscriber.request();
    Assert.assertFalse(subscriber.isDone());

    subscriber.cancel();
    Assert.assertTrue(subscriber.isDone());
    FlowAdapterTest.unwrap(subscriber);
  }

  @Test
  public void testMultiSubscribeInSeries() {
    final List<CloseTrackingIt> its = new ArrayList<>();
    final Publisher<Integer> p = FlowAdapter.toPublisher(() -> {
      final CloseTrackingIt it = new CloseTrackingIt(5);
      its.add(it);
      return it;
    });

    // subscribe twice in series
    final int firstSum =
        FlowAdapter.toAsyncIterator(p).fold(0, (i, j) -> i + j).toCompletableFuture().join();
    final int secondSum =
        FlowAdapter.toAsyncIterator(p).fold(0, (i, j) -> i + j).toCompletableFuture().join();

    Assert.assertEquals(2, its.size());
    Assert.assertTrue(its.stream().allMatch(it -> it.closed));
    Assert.assertEquals(15, secondSum);
    Assert.assertEquals(15, firstSum);
  }

  @Test
  public void testMultiSubscribeInParallel() {
    final List<CloseTrackingIt> its = new ArrayList<>();
    final Publisher<Integer> p = FlowAdapter.toPublisher(() -> {
      final CloseTrackingIt it = new CloseTrackingIt(5);
      its.add(it);
      return it;
    });

    // subscribe twice in parallel
    final AsyncIterator<Integer> it1 = FlowAdapter.toAsyncIterator(p);
    Assert.assertEquals(0, it1.nextStage().toCompletableFuture().join().right().get().intValue());

    final AsyncIterator<Integer> it2 = FlowAdapter.toAsyncIterator(p);
    Assert.assertEquals(0, it2.nextStage().toCompletableFuture().join().right().get().intValue());

    final int firstSum = it1.fold(0, (i, j) -> i + j).toCompletableFuture().join();
    final int secondSum = it2.fold(0, (i, j) -> i + j).toCompletableFuture().join();

    Assert.assertEquals(2, its.size());
    Assert.assertTrue(its.stream().allMatch(it -> it.closed));
    Assert.assertEquals(15, secondSum);
    Assert.assertEquals(15, firstSum);
  }

  private static class CloseTrackingIt implements AsyncIterator<Integer> {
    final int max;
    int i = 0;
    boolean closed = false;

    private CloseTrackingIt(final int max) {
      this.max = max;
    }

    @Override
    public CompletionStage<Either<End, Integer>> nextStage() {
      final int ret = this.i++;
      return StageSupport.completedStage(ret > this.max ? End.end() : Either.right(ret));
    }

    @Override
    public CompletionStage<Void> close() {
      this.closed = true;
      return StageSupport.voidStage();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleSubscription() throws Throwable {
    final Flow.Publisher<Long> publisher = FlowAdapter.toPublisher(AsyncIterator.range(0, 5));
    final ConsumingSubscriber<Long> s = new ConsumingSubscriber<>();
    publisher.subscribe(s);
    s.join();

    final ConsumingSubscriber<Long> s2 = new ConsumingSubscriber<>();
    try {
      publisher.subscribe(s2);
    } catch (final Throwable e) {
      Assert.fail("failure should be notified via onError, got: " + e);
    }
    FlowAdapterTest.unwrap(s2);
  }

  private static void consume(final AsyncIterator<Long> it) throws Throwable {
    final Publisher<Long> publisher = FlowAdapter.toPublisher(it);
    final ConsumingSubscriber<Long> stage = new ConsumingSubscriber<>();
    publisher.subscribe(stage);
    FlowAdapterTest.unwrap(stage);

  }

  private static class ExceptionalIterator implements AsyncIterator<Long> {
    private long count = 0;
    private final long numElements;
    private final Throwable iterationException;
    private final Throwable closeException;

    ExceptionalIterator(final long numElements, final Throwable iterationException,
        final Throwable closeException) {
      this.numElements = numElements;
      this.iterationException = iterationException;
      this.closeException = closeException;

    }

    @Override
    public CompletionStage<Either<End, Long>> nextStage() {
      final long nxt = this.count++;
      if (nxt >= this.numElements) {
        if (this.iterationException != null) {
          return StageSupport.exceptionalStage(this.iterationException);
        }
        return End.endStage();
      }
      return StageSupport.completedStage(Either.right(nxt));
    }

    @Override
    public CompletionStage<Void> close() {
      if (this.closeException != null) {
        return StageSupport.exceptionalStage(this.closeException);
      }
      return StageSupport.voidStage();
    }

  }

  private static <T> void unwrap(final CompletableFuture<T> future) throws Throwable {
    try {
      future.join();
    } catch (final CompletionException e) {
      throw e.getCause();
    }
  }

  private static class ConsumingSubscriber<T> extends CompletableFuture<Void>
      implements Flow.Subscriber<T> {

    private Subscription subscription;

    @Override
    public void onComplete() {
      complete(null);

    }

    @Override
    public void onError(final Throwable arg0) {
      completeExceptionally(arg0);

    }

    @Override
    public void onNext(final T arg0) {
      this.subscription.request(1);
    }

    @Override
    public void onSubscribe(final Subscription arg0) {
      this.subscription = arg0;
      this.subscription.request(1);
    }

    void cancel() {
      this.subscription.cancel();
    }

    void request() {
      this.subscription.request(1);
    }
  }
}


