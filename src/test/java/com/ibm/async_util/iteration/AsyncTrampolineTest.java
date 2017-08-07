/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.iteration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.async_util.util.StageSupport;


public class AsyncTrampolineTest {

  @Test
  public void testExceptionalStackUnroll() {
    final AtomicInteger sum = new AtomicInteger();
    final int breakPoint = 1000000; // enough to cause StackOverflow if broken

    final CompletionStage<Integer> sumFuture = AsyncTrampoline.asyncWhile(
        c -> c < breakPoint,
        c -> {
          sum.addAndGet(c);
          return StageSupport.completedStage(c + 1);
        },
        0);
    final int expected = IntStream.range(0, breakPoint).sum();
    sumFuture.toCompletableFuture().join();

    Assert.assertEquals(expected, sum.get());
  }

  @Test
  public void testThreadsABA() throws Exception {
    final Executor execA = Executors.newSingleThreadExecutor();
    final Thread threadA = CompletableFuture.supplyAsync(() -> Thread.currentThread(), execA).get();

    final Executor execB = Executors.newSingleThreadExecutor();
    final Thread threadB = CompletableFuture.supplyAsync(() -> Thread.currentThread(), execB).get();

    final int sum = CompletableFuture.supplyAsync(
        () -> AsyncTrampoline.asyncWhile(
            c -> c < 3,
            c -> {
              final CompletableFuture<Integer> future = new CompletableFuture<>();
              final Thread currentThread = Thread.currentThread();
              if (currentThread.equals(threadA)) {
                // switch to B
                execB.execute(() -> future.complete(c + 1));
              } else {
                Assert.assertEquals(threadB, currentThread);
                // switch back to A
                execA.execute(() -> future.complete(c + 1));
              }
              return future;
            },
            0),
        execA).get(10, TimeUnit.SECONDS).toCompletableFuture().get(10, TimeUnit.SECONDS);

    Assert.assertEquals(3, sum);
  }

  @Test
  public void testThreadsAA() throws Exception {
    final Executor execA = Executors.newSingleThreadExecutor();

    final int sum = CompletableFuture.supplyAsync(
        () -> AsyncTrampoline.asyncWhile(
            c -> c < 3,
            c -> {
              final CompletableFuture<Integer> future = new CompletableFuture<>();
              execA.execute(() -> future.complete(c + 1));
              return future;
            },
            0),
        execA).get(5, TimeUnit.SECONDS).toCompletableFuture().get(5, TimeUnit.SECONDS);

    Assert.assertEquals(3, sum);
  }

}
