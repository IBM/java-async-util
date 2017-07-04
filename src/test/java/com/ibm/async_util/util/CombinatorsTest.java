package com.ibm.async_util.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
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

public class CombinatorsTest {
  private static class TestException extends RuntimeException {}

  @Test
  @SuppressWarnings("unchecked")
  public void testAllOf() {
    final List<Integer> results = Arrays.asList(1, 2, 3);
    final CompletableFuture[] arr =
        results.stream().map(CompletableFuture::completedFuture).toArray(CompletableFuture[]::new);
    final List<CompletableFuture<Integer>> collect =
        results.stream().map(CompletableFuture::completedFuture).collect(Collectors.toList());

    Assert.assertEquals(results, Combinators.allOf(arr).toCompletableFuture().join());
    Assert.assertEquals(results, Combinators.allOf(collect).toCompletableFuture().join());
    Assert.assertEquals(results, Combinators.collect(collect, Collectors.toList()).toCompletableFuture().join());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllOfError() {
    List<CompletableFuture<Integer>> futures =
        Arrays.asList(
            CompletableFuture.completedFuture(1),
            FutureSupport.<Integer>errorStage(new TestException()).toCompletableFuture());
    assertError(Combinators.allOf(futures.toArray(new CompletableFuture[0])));
    assertError(Combinators.allOf(futures));
    assertError(Combinators.collect(futures, Collectors.toList()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllOfErrorNoShortCircuit() {
    final CompletableFuture<Integer> delayed = new CompletableFuture<>();
    List<CompletableFuture<Integer>> futures =
        Arrays.asList(
            delayed, FutureSupport.<Integer>errorStage(new TestException()).toCompletableFuture());

    CompletionStage<Collection<Integer>> arrAll =
        Combinators.allOf(futures.toArray(new CompletableFuture[0]));
    CompletionStage<Collection<Integer>> collAll = Combinators.allOf(futures);
    CompletionStage<List<Integer>> collCollect = Combinators.collect(futures, Collectors.toList());

    assertIncomplete(arrAll);
    assertIncomplete(collAll);
    assertIncomplete(collCollect);

    delayed.complete(1);

    assertError(arrAll);
    assertError(collAll);
    assertError(collCollect);
  }

  @Test
  public void testKeyedAll() {
    final Map<Integer, CompletionStage<Integer>> stageMap =
        IntStream.range(0, 5)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), CompletableFuture::completedFuture));
    final Map<Integer, Integer> integerMap = Combinators.keyedAll(stageMap).toCompletableFuture().join();
    Assert.assertEquals(5, integerMap.size());
    Assert.assertTrue(integerMap.entrySet().stream().allMatch(entry -> entry.getKey().equals(entry.getValue())));
  }

  @Test
  public void testKeyedAllError() {
    final Map<Integer, CompletionStage<Integer>> stageMap =
            IntStream.range(0, 5)
                    .boxed()
                    .collect(Collectors.toMap(Function.identity(), i -> {
                      if (i == 3) {
                        return FutureSupport.errorStage(new TestException());
                      }
                      return CompletableFuture.completedFuture(i);
                    }));
    assertError(Combinators.keyedAll(stageMap));
  }

  @Test
  public void testKeyedAllErrorNoShortCircuit() {
    final Map<Integer, CompletableFuture<Integer>> stageMap =
            IntStream.range(0, 5)
                    .boxed()
                    .collect(Collectors.toMap(Function.identity(), i -> new CompletableFuture<>()));
    CompletionStage<Map<Integer, Integer>> fut = Combinators.keyedAll(stageMap);
    int i = 0;
    for (CompletableFuture<Integer> future : stageMap.values()) {
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
    } catch (CompletionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException);
    }
  }

  private <T> void assertIncomplete(final CompletionStage<T> stage) {
    try {
      stage.toCompletableFuture().get(20, TimeUnit.MILLISECONDS);
      Assert.fail("not all futures complete, get should timeout");
    } catch (InterruptedException e) {
    } catch (ExecutionException e) {
      Assert.fail(e.getMessage());
    } catch (TimeoutException e) {
      // expected
    }
  }
}
