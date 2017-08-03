/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Utility methods for combining more than one {@link CompletionStage} into a single
 * {@link CompletionStage}
 */
public class Combinators {
  private Combinators() {}

  /**
   * Given a collection of stages all of the same type, returns a new {@link CompletionStage} that
   * is completed with the result of all input stages when all stages are complete. If the input
   * collection has a defined order, the order will be preserved in the returned collection. If an
   * element of {@code stages} completes exceptionally, so too will the CompletionStage returned by
   * this method.
   *
   * @param stages a Collection of {@link CompletionStage} all of type T
   * @return a {@link CompletionStage} which will complete with a collection of the elements
   *         produced by {@code stages} when all stages complete
   * @throws NullPointerException if {@code stages} or any of its elements are null
   */
  @SuppressWarnings("unchecked")
  public static <T> CompletionStage<Collection<T>> collectAll(
      final Collection<? extends CompletionStage<T>> stages) {
    final CompletableFuture<T>[] futures = stages.toArray(new CompletableFuture[0]);
    return CompletableFuture
        .allOf(futures)
        .thenApply(
            ig -> Arrays
                .stream(futures)
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
  }

  /**
   * Given a collection of stages, returns a new {@link CompletionStage} that is completed when all
   * input stages are complete. If any stage completes exceptionally, the returned stage will
   * complete exceptionally.
   *
   * @param stages a Collection of {@link CompletionStage}
   * @return a {@link CompletionStage} which will complete after every stage in {@code stages}
   *         completes
   * @throws NullPointerException if {@code stages} or any of its elements are null
   */
  @SuppressWarnings("unchecked")
  public static CompletionStage<Void> allOf(
      final Collection<? extends CompletionStage<?>> stages) {
    final CompletableFuture[] futures = stages.toArray(new CompletableFuture[0]);
    return CompletableFuture.allOf(futures);
  }

  /**
   * Given a Map from some key type K to {@link CompletionStage CompletionStages} of values, returns
   * a {@link CompletionStage} which completes with a {@code Map<K, V>} when all the
   * CompletionStages in the input map have completed. For example, if we have an asynchronous
   * method to lookup student grade point averages.
   *
   * <pre>
   * {@code
   * Map<Student, CompletionStage<Double>> gpaFutures =
   *  students
   *      .stream()
   *      .collect(Collectors.toMap(Functions.identity(), student -> getGpaAsync(student));
   * Map<Student, Double> studentGpas = keyedAll(gpaFutures).toCompletableFuture().join();
   * }
   * </pre>
   *
   * If a value in {@code stageMap} completes exceptionally, so too will the CompletionStage
   * returned by this method.
   *
   * @param stageMap a Map with keys of type K and {@link CompletionStage CompletionStages} of type
   *        V for values
   * @param <K> the input and output key type
   * @param <V> the value type for the map that will be produced by the returned
   *        {@link CompletionStage}
   * @throws NullPointerException if {@code stageMap} or any of its values are null
   * @return a {@link CompletionStage} that will be completed with a map mapping keys of type K to
   *         the values returned by the CompletionStages in {@code stageMap}
   */
  @SuppressWarnings("unchecked")
  public static <K, V> CompletionStage<Map<K, V>> keyedAll(
      final Map<K, ? extends CompletionStage<V>> stageMap) {
    return Combinators
        .allOf(stageMap.values())
        .thenApply(ignore -> stageMap.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toCompletableFuture().join())));
  }

  /**
   * Applies a collector to the results of all {@code stages} after all complete, returning a
   * {@link CompletionStage} of the collected result. There is no need nor benefit for the Collector
   * to have the {@link java.util.stream.Collector.Characteristics CONCURRENT characteristic}, the
   * {@code collector} will be applied in a single thread. If any of the input stages completes
   * exceptionally, so too will the CompletionStage returned by this method.
   *
   * @param stages a Collection of stages all of type T
   * @param collector a {@link Collector} which will be applied to the results of {@code stages} to
   *        produce the final R result.
   * @param <T> The type of the elements in {@code stages} which will be collected by {@code
   *     collector}
   * @param <A> The intermediate collection type
   * @param <R> The final type returned by {@code collector}
   * @return a {@link CompletionStage} which will complete with the R typed object that is produced
   *         by {@code collector} when all input {@code stages} have completed.
   * @throws NullPointerException if {@code stages} or any of its elements are null
   */
  @SuppressWarnings("unchecked")
  public static <T, A, R> CompletionStage<R> collect(
      final Collection<? extends CompletionStage<T>> stages,
      final Collector<? super T, A, R> collector) {
    return collectAll(stages).thenApply(res -> res.stream().collect(collector));
  }
}
