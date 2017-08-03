/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

public class CombinatorsTest {
  private static class TestException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllOf() {
    final List<Integer> results = Arrays.asList(1, 2, 3);
    final List<CompletionStage<Integer>> collect =
        results.stream().map(StageSupport::completedStage).collect(Collectors.toList());

    Combinators.allOf(collect).toCompletableFuture().join();
    Assert.assertArrayEquals(results.toArray(),
        Combinators.collect(collect).toCompletableFuture().join().toArray());
    Assert.assertArrayEquals(results.toArray(),
        Combinators.collect(collect, Collectors.toList()).toCompletableFuture().join().toArray());
  }

  @Test
  public void testAllEmptyInput()
      throws InterruptedException, ExecutionException, TimeoutException {
    {
      final Collection<Object> collection = Combinators.collect(Collections.emptyList())
          .toCompletableFuture().get(50, TimeUnit.MILLISECONDS);
      Assert.assertTrue(collection.isEmpty());
    }

    {
      final Collection<Object> collection =
          Combinators.collect(Collections.emptyList(), Collectors.toList()).toCompletableFuture()
              .get(50, TimeUnit.MILLISECONDS);
      Assert.assertTrue(collection.isEmpty());
    }

    {
      Combinators.allOf(Collections.emptyList()).toCompletableFuture().join();
    }
  }

  @Test
  public void testAllLargeCollection() {
    final int NUM_FUTURES = 10000;

    final List<Integer> results =
        IntStream.range(0, NUM_FUTURES).boxed().collect(Collectors.toList());
    final List<CompletionStage<Integer>> collect = IntStream
        .range(0, NUM_FUTURES)
        .mapToObj(StageSupport::completedStage)
        .collect(Collectors.toList());

    Combinators.allOf(collect).toCompletableFuture().join();
    Assert.assertArrayEquals(results.toArray(),
        Combinators.collect(collect).toCompletableFuture().join().toArray());
    Assert.assertArrayEquals(results.toArray(),
        Combinators.collect(collect, Collectors.toList()).toCompletableFuture().join().toArray());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllOfError() {
    final List<CompletableFuture<Integer>> futures =
        Arrays.asList(
            StageSupport.completedStage(1).toCompletableFuture(),
            StageSupport.<Integer>exceptionalStage(new TestException()).toCompletableFuture());
    assertError(Combinators.allOf(futures));
    assertError(Combinators.collect(futures));
    assertError(Combinators.collect(futures, Collectors.toList()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllOfErrorNoShortCircuit() {
    final CompletableFuture<Integer> delayed = new CompletableFuture<>();
    final List<CompletableFuture<Integer>> futures =
        Arrays.asList(
            delayed,
            StageSupport.<Integer>exceptionalStage(new TestException()).toCompletableFuture());

    final CompletionStage<Void> voidAll = Combinators.allOf(futures);
    final CompletionStage<Collection<Integer>> collAll = Combinators.collect(futures);
    final CompletionStage<List<Integer>> collCollect =
        Combinators.collect(futures, Collectors.toList());

    assertIncomplete(voidAll);
    assertIncomplete(collAll);
    assertIncomplete(collCollect);

    delayed.complete(1);

    assertError(voidAll);
    assertError(collAll);
    assertError(collCollect);
  }

  @Test
  public void testKeyedAll() {
    final Map<Integer, CompletionStage<Integer>> stageMap =
        IntStream.range(0, 5)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), StageSupport::completedStage));
    final Map<Integer, Integer> integerMap =
        Combinators.keyedAll(stageMap).toCompletableFuture().join();
    Assert.assertEquals(5, integerMap.size());
    Assert.assertTrue(
        integerMap.entrySet().stream().allMatch(entry -> entry.getKey().equals(entry.getValue())));
  }

  @Test
  public void testKeyedAllError() {
    final Map<Integer, CompletionStage<Integer>> stageMap =
        IntStream.range(0, 5)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), i -> {
              if (i == 3) {
                return StageSupport.exceptionalStage(new TestException());
              }
              return StageSupport.completedStage(i);
            }));
    assertError(Combinators.keyedAll(stageMap));
  }

  @Test
  public void testKeyedAllErrorNoShortCircuit() {
    final Map<Integer, CompletableFuture<Integer>> stageMap =
        IntStream.range(0, 5)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), i -> new CompletableFuture<>()));
    final CompletionStage<Map<Integer, Integer>> fut = Combinators.keyedAll(stageMap);
    int i = 0;
    for (final CompletableFuture<Integer> future : stageMap.values()) {
      assertIncomplete(fut);
      if (i == 3) {
        future.completeExceptionally(new TestException());
      } else {
        future.complete(i);
      }
      i++;
    }
    assertError(fut);
  }

  private <T> void assertError(final CompletionStage<T> stage) {
    try {
      stage.toCompletableFuture().join();
    } catch (final CompletionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException);
    }
  }

  private <T> void assertIncomplete(final CompletionStage<T> stage) {
    try {
      stage.toCompletableFuture().get(20, TimeUnit.MILLISECONDS);
      Assert.fail("not all futures complete, get should timeout");
    } catch (final InterruptedException e) {
    } catch (final ExecutionException e) {
      Assert.fail(e.getMessage());
    } catch (final TimeoutException e) {
      // expected
    }
  }
}
