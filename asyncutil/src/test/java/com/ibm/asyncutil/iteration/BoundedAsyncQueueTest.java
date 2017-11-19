/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.iteration;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BoundedAsyncQueueTest extends AbstractAsyncQueueTest {

  private BoundedAsyncQueue<Integer> queue;

  @Before
  public void makeQueue() {
    this.queue = AsyncQueues.bounded();
  }

  @Override
  boolean send(final Integer c) {
    return this.queue.send(c).toCompletableFuture().join();
  }

  @Override
  AsyncIterator<Integer> consumer() {
    return this.queue;
  }

  @Override
  void closeImpl() {
    this.queue.terminate();
  }

  @Test
  public void asyncCloseContractTest() {
    // accepted right away
    final CompletableFuture<Boolean> f1 = this.queue.send(1).toCompletableFuture();
    Assert.assertTrue(f1.isDone());
    Assert.assertTrue(f1.join());

    // waiting
    final CompletableFuture<Boolean> f2 = this.queue.send(2).toCompletableFuture();
    Assert.assertFalse(f2.isDone());

    // terminate
    final CompletableFuture<Void> closeFuture = this.queue.terminate().toCompletableFuture();

    Assert.assertFalse(f2.isDone());
    Assert.assertFalse(closeFuture.isDone());

    // send after terminate
    final CompletableFuture<Boolean> f3 = this.queue.send(3).toCompletableFuture();

    // consume a result
    Assert.assertEquals(1,
        this.queue.nextStage().toCompletableFuture().join().right().get().intValue());

    // f2 should be done, and accepted
    Assert.assertTrue(f2.isDone());
    Assert.assertTrue(f2.join());

    Assert.assertEquals(2,
        this.queue.nextStage().toCompletableFuture().join().right().get().intValue());
    Assert.assertFalse(this.queue.nextStage().toCompletableFuture().join().isRight());

    // terminate should be done, f3 should be done and rejected
    Assert.assertTrue(closeFuture.isDone());
    Assert.assertTrue(f3.isDone());
    Assert.assertFalse(f3.join());
  }

  @Override
  Optional<Integer> poll() {
    return this.queue.poll();
  }
}


