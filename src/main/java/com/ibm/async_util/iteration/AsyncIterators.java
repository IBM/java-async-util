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

package com.ibm.async_util.iteration;

import com.ibm.async_util.iteration.AsyncIterator.End;
import com.ibm.async_util.util.Either;
import com.ibm.async_util.util.FutureSupport;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/** Package private methods to use in {@link AsyncIterator} */
class AsyncIterators {

  private AsyncIterators() {}

  private static final Either<End, ?> ITERATION_END =
      Either.left(
          new AsyncIterator.End() {
            @Override
            public String toString() {
              return "End of iteration";
            }
          });

  private static final CompletionStage<? extends Either<AsyncIterator.End, ?>> END_FUTURE =
      CompletableFuture.completedFuture(ITERATION_END);

  static final EmptyAsyncIterator<?> EMPTY_ITERATOR = new EmptyAsyncIterator<>();

  @SuppressWarnings("unchecked")
  static <T> Either<AsyncIterator.End, T> end() {
    return (Either<AsyncIterator.End, T>) ITERATION_END;
  }

  @SuppressWarnings("unchecked")
  static <T> CompletionStage<Either<AsyncIterator.End, T>> endFuture() {
    return (CompletionStage<Either<AsyncIterator.End, T>>) END_FUTURE;
  }

  private static class EmptyAsyncIterator<T> implements AsyncIterator<T> {
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

  /** Complete dest with whatever result (T or a Throwable) comes out of source */
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

  static <T> CompletionStage<Either<End, T>> convertSynchronousException(
      final Supplier<CompletionStage<Either<End, T>>> supplier) {
    try {
      return supplier.get();
    } catch (Throwable e) {
      return FutureSupport.errorStage(e);
    }
  }

  /** If both et and eu are right, then compute a new right either, otherwise just return left */
  static <T, U, V> Either<AsyncIterator.End, V> zipWith(
      Either<AsyncIterator.End, T> et,
      Either<AsyncIterator.End, U> eu,
      final BiFunction<T, U, V> f) {
    return et.fold(end -> end(), t -> eu.fold(end -> end(), u -> Either.right(f.apply(t, u))));
  }

  static <T, U> AsyncIterator<U> thenApplyImpl(
      AsyncIterator<T> it, Function<? super T, ? extends U> f, final Executor e) {
    return e == null
        ? () -> it.nextFuture().thenApply(either -> either.map(f))
        : () -> it.nextFuture().thenApplyAsync(either -> either.map(f), e);
  }

  static <T, U> AsyncIterator<U> thenComposeImpl(
      final AsyncIterator<T> it, Function<T, CompletionStage<U>> f, final Executor e) {
    return () -> {
      CompletionStage<Either<End, T>> nxt = it.nextFuture();
      // if there's a value, apply f and wrap the result in an Either,
      // otherwise just return end marker
      return e == null
          ? nxt.thenCompose(
              nt -> nt.fold(end -> endFuture(), t -> f.apply(t).thenApply(Either::right)))
          : nxt.thenComposeAsync(
              nt -> nt.fold(end -> endFuture(), t -> f.apply(t).thenApply(Either::right)), e);
    };
  }
}
