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

import com.ibm.async_util.AsyncIterator.End;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collector;

/** Package private methods to use in {@link AsyncIterator} */
class AsyncIterators {

  private AsyncIterators() {}

  private static final Either<End, ?> ITERATION_END =
      Either.left(
          new End() {
            @Override
            public String toString() {
              return "End of iteration";
            }
          });

  private static final CompletionStage<? extends Either<End, ?>> END_FUTURE =
      CompletableFuture.completedFuture(ITERATION_END);

  static final EmptyAsyncIterator<?> EMPTY_ITERATOR = new EmptyAsyncIterator<>();

  @SuppressWarnings("unchecked")
  static <T> Either<End, T> end() {
    return (Either<End, T>) ITERATION_END;
  }

  @SuppressWarnings("unchecked")
  static <T> CompletionStage<Either<End, T>> endFuture() {
    return (CompletionStage<Either<End, T>>) END_FUTURE;
  }

  private static class EmptyAsyncIterator<T> extends AsyncIterator<T> {
    @Override
    public CompletionStage<Either<End, T>> nextFuture() {
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
        ? ((R) accumulator)
        : collector.finisher().apply(accumulator);
  }

  static <T> CompletableFuture<T> exceptional(final Throwable ex) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    completableFuture.completeExceptionally(ex);
    return completableFuture;
  }

  static <T> void listen(final CompletionStage<T> source, final CompletableFuture<T> dest) {
    source.whenComplete(
        (t, ex) -> {
          if (t != null) {
            dest.complete(t);
          } else {
            dest.completeExceptionally(ex);
          }
        });
  }

  static <T, U, V> Either<End, V> zipWith(
      Either<End, T> et, Either<End, U> eu, final BiFunction<T, U, V> f) {
    return et.fold(end -> end(), t -> eu.fold(end -> end(), u -> Either.right(f.apply(t, u))));
  }
}
