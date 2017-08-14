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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.ibm.async_util.util.TestUtil.CompletableStage;

@RunWith(Parameterized.class)
public class CombinatorsTest {
  private static class TestException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  @Parameters(name = "{index}: stage impls <{0}>, <{1}>")
  public static Collection<Object[]> stageFactories() {
    return TestUtil.stageFactories()
        .flatMap(completableSupplier -> TestUtil.doneStageFactories()
            .map(completedFunction -> new Object[] {completableSupplier, completedFunction}))
        .collect(Collectors.toList());
  }

  @Parameter(0)
  public Supplier<CompletableStage<?>> stageFactory;
  @Parameter(1)
  public Function<Either<Throwable, ?>, CompletionStage<?>> completedStageFactory;

  @SuppressWarnings("unchecked")
  private <T> CompletableStage<T> getCompletableStage() {
    return (CompletableStage<T>) this.stageFactory.get();
  }

  @SuppressWarnings("unchecked")
  private <T> CompletionStage<T> getCompletedStage(final T result) {
    return (CompletionStage<T>) this.completedStageFactory.apply(Either.right(result));
  }

  @SuppressWarnings("unchecked")
  private <T> CompletionStage<T> getExceptionalStage(final Throwable exc) {
    return (CompletionStage<T>) this.completedStageFactory.apply(Either.left(exc));
  }


  @Test
  public void testAllOf() {
    final List<Integer> results = Arrays.asList(1, 2, 3);
    final List<CompletionStage<Integer>> collect =
        results.stream().map(this::getCompletedStage).collect(Collectors.toList());

    TestUtil.join(Combinators.allOf(collect));
    Assert.assertArrayEquals(results.toArray(),
        TestUtil.join(Combinators.collect(collect)).toArray());
    Assert.assertArrayEquals(results.toArray(),
        TestUtil.join(Combinators.collect(collect, Collectors.toList())).toArray());
  }

  @Test
  public void testAllEmptyInput()
      throws TimeoutException {
    {
      final Collection<Object> collection =
          TestUtil.join(Combinators.collect(Collections.emptyList()), 50, TimeUnit.MILLISECONDS);
      Assert.assertTrue(collection.isEmpty());
    }

    {
      final Collection<Object> collection =
          TestUtil.join(Combinators.collect(Collections.emptyList(), Collectors.toList()),
              50, TimeUnit.MILLISECONDS);
      Assert.assertTrue(collection.isEmpty());
    }

    {
      TestUtil.join(Combinators.allOf(Collections.emptyList()));
    }
  }

  @Test
  public void testAllLargeCollection() {
    final int NUM_FUTURES = 10000;

    final List<Integer> results =
        IntStream.range(0, NUM_FUTURES).boxed().collect(Collectors.toList());
    final List<CompletionStage<Integer>> collect = IntStream
        .range(0, NUM_FUTURES)
        .mapToObj(this::getCompletedStage)
        .collect(Collectors.toList());

    TestUtil.join(Combinators.allOf(collect));
    Assert.assertArrayEquals(results.toArray(),
        TestUtil.join(Combinators.collect(collect)).toArray());
    Assert.assertArrayEquals(results.toArray(),
        TestUtil.join(Combinators.collect(collect, Collectors.toList())).toArray());
  }

  @Test
  public void testAllOfError() {
    final List<CompletionStage<Integer>> futures =
        Arrays.asList(
            getCompletedStage(1),
            getExceptionalStage(new TestException()));
    assertError(Combinators.allOf(futures));
    assertError(Combinators.collect(futures));
    assertError(Combinators.collect(futures, Collectors.toList()));
  }

  @Test
  public void testAllOfErrorNoShortCircuit() {
    final CompletableStage<Integer> delayed = getCompletableStage();
    final List<CompletionStage<Integer>> futures =
        Arrays.asList(
            delayed,
            getExceptionalStage(new TestException()));

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
            .collect(Collectors.toMap(Function.identity(), this::getCompletedStage));
    final Map<Integer, Integer> integerMap =
        TestUtil.join(Combinators.keyedAll(stageMap));
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
                return getExceptionalStage(new TestException());
              }
              return getCompletedStage(i);
            }));
    assertError(Combinators.keyedAll(stageMap));
  }

  @Test
  public void testKeyedAllErrorNoShortCircuit() {
    final Map<Integer, CompletableStage<Integer>> stageMap =
        IntStream.range(0, 5)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), i -> getCompletableStage()));
    final CompletionStage<Map<Integer, Integer>> fut = Combinators.keyedAll(stageMap);
    int i = 0;
    for (final CompletableStage<Integer> future : stageMap.values()) {
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
      TestUtil.join(stage);
    } catch (final CompletionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException);
    }
  }

  private <T> void assertIncomplete(final CompletionStage<T> stage) {
    try {
      TestUtil.join(stage, 20, TimeUnit.MILLISECONDS);
      Assert.fail("not all futures complete, get should timeout");
    } catch (final TimeoutException e) {
      // expected
    }
  }
}
