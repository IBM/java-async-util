package com.ibm.async_util.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Combinators {
  private Combinators() {}

  @SafeVarargs
  public static <T> CompletionStage<Collection<T>> all(final CompletableFuture<T>... futures) {
    return CompletableFuture.allOf(futures)
        .thenApply(
            ignored ->
                Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  public static <T> CompletionStage<Collection<T>> all(
      final Collection<? extends CompletionStage<T>> futures) {
    return all(
        futures
            .stream()
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new));
  }

  public static <K, V> CompletionStage<Map<K, V>> keyedAll(
      final Map<K, CompletionStage<V>> futures) {
    return CompletableFuture.allOf(futures.values().stream().toArray(CompletableFuture[]::new))
        .thenApply(
            ignore ->
                futures
                    .entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(
                            e -> e.getKey(), e -> e.getValue().toCompletableFuture().join())));
  }

  @SuppressWarnings("unchecked")
  public static <T, A, R> CompletionStage<R> collect(
      final Collection<CompletionStage<T>> futures, final Collector<T, A, R> collector) {
    @SuppressWarnings("rawtypes")
    CompletableFuture[] arr =
        futures
            .stream()
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(arr)
        .thenApply(
            ignored -> {
              return Stream.of((CompletableFuture<T>[]) arr)
                  .map(CompletableFuture::join)
                  .collect(collector);
            });
  }
}
