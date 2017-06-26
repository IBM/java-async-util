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
// Date: Jun 18, 2015
// ---------------------

package com.ibm.async_util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collector;

import com.ibm.async_util.AsyncIterator.End;

/**
 * Package private methods to use in {@link AsyncIterator}
 */
class AsyncIterators {

  private AsyncIterators() {}
  
  private static final Either<?, End> ITERATION_END = Either.right(new End(){});

  private static final CompletionStage<? extends Either<?, End>> END_FUTURE =
      CompletableFuture.completedFuture(ITERATION_END);

  static final EmptyAsyncIterator<?> EMPTY_ITERATOR =
      new EmptyAsyncIterator<>();

  @SuppressWarnings("unchecked")
  static <T> Either<T, End> end() {
    return (Either<T, End>) ITERATION_END;
  }

  @SuppressWarnings("unchecked")
  static <T> CompletionStage<Either<T, End>> endFuture() {
    return (CompletionStage<Either<T, End>>) END_FUTURE;
  }

  private static class EmptyAsyncIterator<T> implements AsyncIterator<T> {
    @Override
    public CompletionStage<Either<T, End>> nextFuture() {
      return AsyncIterators.endFuture();
    }

    @Override
    public String toString() {
      return "EmptyAsyncIterator";
    }
  }


  @SuppressWarnings("unchecked")
  static <A, R> R finishContainer(final A accumulator, final Collector<?, A, R> collector) {
    // cast instead of applying the finishing function if the collector indicates the
    // finishing function is just identity
    return collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)
        ? ((R) accumulator) : collector.finisher().apply(accumulator);
  }

  static <T> CompletableFuture<T> exceptional(final Throwable ex) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    completableFuture.completeExceptionally(ex);
    return completableFuture;
  }


  static <T> void listen(final CompletionStage<T> source, final CompletableFuture<T> dest) {
    source.whenComplete((t, ex) -> {
      if (t != null) {
        dest.complete(t);
      } else {
        dest.completeExceptionally(ex);
      }
    });
  }
  
  static <T, U, V> Either<V, End> zipWith(Either<T, End> et, Either<U, End> eu, final BiFunction<T, U, V> f) {
    return et.fold(t -> eu.fold(u -> Either.left(f.apply(t, u)), end -> end()), end -> end());
  }

}
