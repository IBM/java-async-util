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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import com.ibm.async_util.iteration.AsyncIterator.End;
import com.ibm.async_util.locks.FairAsyncLock;
import com.ibm.async_util.util.Either;
import com.ibm.async_util.util.StageSupport;

/** Package private methods to use in {@link AsyncIterator} */
class AsyncIterators {

  private AsyncIterators() {}

  static final EmptyAsyncIterator<?> EMPTY_ITERATOR = new EmptyAsyncIterator<>();

  private static class EmptyAsyncIterator<T> implements AsyncIterator<T> {

    @Override
    public CompletionStage<Either<End, T>> nextFuture() {
      return End.endFuture();
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
          if (ex != null) {
            dest.completeExceptionally(ex);
          } else {
            dest.complete(t);
          }
        });
  }

  static <T> CompletionStage<T> convertSynchronousException(
      final Supplier<? extends CompletionStage<T>> supplier) {
    try {
      return supplier.get();
    } catch (final Throwable e) {
      return StageSupport.exceptionalStage(e);
    }
  }

  /** If both et and eu are right, then compute a new right either, otherwise just return left */
  static <T, U, V> Either<AsyncIterator.End, V> zipWith(
      final Either<AsyncIterator.End, T> et,
      final Either<AsyncIterator.End, U> eu,
      final BiFunction<? super T, ? super U, V> f) {
    return et.fold(end -> End.end(),
        t -> eu.fold(end -> End.end(), u -> Either.right(f.apply(t, u))));
  }

  static <T, U> AsyncIterator<U> thenApplyImpl(
      final AsyncIterator<T> it,
      final Function<? super T, ? extends U> f,
      final boolean synchronous,
      final Executor e) {
    assert !synchronous || e == null;
    return new AsyncIterator<U>() {
      @Override
      public CompletionStage<Either<End, U>> nextFuture() {
        final CompletionStage<Either<End, T>> next = it.nextFuture();

        return synchronous
            ? next.thenApply(this::eitherFunction)
            : e == null
                ? next.thenApplyAsync(this::eitherFunction)
                : next.thenApplyAsync(this::eitherFunction, e);
      }

      Either<End, U> eitherFunction(final Either<End, T> either) {
        return either.map(f);
      }

      @Override
      public CompletionStage<Void> close() {
        return it.close();
      }
    };
  }

  static <T, U> AsyncIterator<U> thenComposeImpl(
      final AsyncIterator<T> it,
      final Function<? super T, ? extends CompletionStage<U>> f,
      final boolean synchronous,
      final Executor e) {
    assert !synchronous || e == null;

    return new AsyncIterator<U>() {
      @Override
      public CompletionStage<Either<End, U>> nextFuture() {
        final CompletionStage<Either<End, T>> nxt = it.nextFuture();
        return synchronous
            ? nxt.thenCompose(this::eitherFunction)
            : e == null
                ? nxt.thenComposeAsync(this::eitherFunction)
                : nxt.thenComposeAsync(this::eitherFunction, e);
      }

      /*
       * if there's a value, apply f and wrap the result in an Either, otherwise just return end
       * marker
       */
      private CompletionStage<Either<End, U>> eitherFunction(final Either<End, T> either) {
        return either.fold(
            end -> End.endFuture(),
            t -> f.apply(t).thenApply(Either::right));
      }

      @Override
      public CompletionStage<Void> close() {
        return it.close();
      }
    };
  }

  static class PartiallyEagerAsyncIterator<T, U> implements AsyncIterator<U> {
    private final AsyncIterator<T> backingIterator;
    private final int executeAhead;
    private final Function<U, CompletionStage<Void>> closeFn;
    private final Function<Either<End, T>, CompletionStage<Either<End, U>>> mappingFn;
    private final Queue<CompletionStage<Either<End, U>>> pendingResults;
    private final FairAsyncLock lock;
    private boolean closed;


    PartiallyEagerAsyncIterator(
        final AsyncIterator<T> backingIterator,
        final int executeAhead,
        final Function<Either<End, T>, CompletionStage<Either<End, U>>> mappingFn,
        final Function<U, CompletionStage<Void>> closeFn) {
      this.backingIterator = backingIterator;
      this.executeAhead = executeAhead;
      this.closeFn = closeFn == null
          ? u -> StageSupport.voidFuture()
          : u -> AsyncIterators.convertSynchronousException(() -> closeFn.apply(u));
      this.mappingFn = mappingFn;
      this.pendingResults = new ArrayDeque<>(executeAhead);
      this.lock = new FairAsyncLock();
      this.closed = false;
    }

    /* return whether we need to keep filling */
    private CompletionStage<Either<End, T>> fillMore() {
      if (this.pendingResults.size() >= this.executeAhead) {
        // don't call nextFuture, we already have enough stuff pending
        return End.endFuture();
      } else {
        // keep filling up the ahead queue
        final CompletionStage<Either<End, T>> nxt =
            AsyncIterators.convertSynchronousException(this.backingIterator::nextFuture);
        this.pendingResults.add(nxt.thenCompose(this.mappingFn));
        return nxt;
      }
    }

    /**
     * Get a nextFuture either from the queue or the backing iterator and apply the mappingFn.
     *
     * @param listener stage is completed when the mapping function finishes
     * @return stage that is complete when any calls that this method made to nextFuture are
     *         complete
     */
    private CompletionStage<Void> attachListener(final CompletableFuture<Either<End, U>> listener) {
      return StageSupport.tryComposeWith(this.lock.acquireLock(), token -> {
        if (this.closed) {
          final IllegalStateException ex =
              new IllegalStateException("nextFuture called after async iterator was closed");
          listener.completeExceptionally(ex);
          throw ex;
        }

        final CompletionStage<Either<End, U>> poll = this.pendingResults.poll();
        if (poll == null) {

          // there was nothing in the queue, associate our returned future with a new
          // safeNextFuture call
          final CompletionStage<Either<End, T>> nxt =
              AsyncIterators.convertSynchronousException(this.backingIterator::nextFuture);

          // don't bother adding it to the queue, because we are already listening on it
          AsyncIterators.listen(nxt.thenCompose(this.mappingFn), listener);

          return StageSupport.voided(nxt);
        } else {
          // let our future be tied to the first result that was in the queue
          AsyncIterators.listen(poll, listener);
          return StageSupport.voidFuture();
        }
      });
    }

    @Override
    public CompletionStage<Either<End, U>> nextFuture() {
      final CompletableFuture<Either<End, U>> listener = new CompletableFuture<>();
      final CompletionStage<Void> nextFinished = attachListener(listener);

      nextFinished.thenRun(() -> {
        AsyncTrampoline
            .asyncWhile(() -> StageSupport.tryComposeWith(this.lock.acquireLock(), token -> {
              if (this.closed) {
                return StageSupport.completedStage(false);
              }
              return fillMore()
                  .thenApply(Either::isRight)
                  // exceptional futures get added to the queue same as normal ones,
                  // we may continue filling
                  .exceptionally(e -> true);
            }));
      });

      return listener;
    }

    /*
     * wait for all pending results and then call close. epoch guarantees no more new results will
     * come in
     */
    @Override
    public CompletionStage<Void> close() {
      return StageSupport.tryComposeWith(this.lock.acquireLock(), token -> {
        this.closed = true;
        // call closeFn on all extra eagerly evaluated results
        @SuppressWarnings({"rawtypes"})
        final CompletableFuture[] closeFutures =
            this.pendingResults
                .stream()
                .map(f -> f.thenCompose(
                    either -> either.fold(
                        end -> StageSupport.voidFuture(),
                        this.closeFn)))
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new);

        // wait for all to complete
        final CompletableFuture<Void> extraClose = CompletableFuture.allOf(closeFutures);
        return StageSupport.thenComposeOrRecover(
            extraClose,
            (ig, extraCloseError) -> {
              // call close on the source iterator
              return StageSupport.thenComposeOrRecover(
                  AsyncIterators.convertSynchronousException(this.backingIterator::close),
                  (ig2, backingCloseError) -> {
                    if (extraCloseError != null) {
                      return StageSupport.<Void>exceptionalStage(extraCloseError);
                    } else if (backingCloseError != null) {
                      return StageSupport.<Void>exceptionalStage(backingCloseError);
                    }
                    return StageSupport.voidFuture();
                  });
            });
      });
    }
  }

  private static class FailOnceAsyncIterator<T> implements AsyncIterator<T> {
    private Throwable exception;

    FailOnceAsyncIterator(final Throwable e) {
      this.exception = Objects.requireNonNull(e);
    }

    @Override
    public CompletionStage<Either<End, T>> nextFuture() {
      if (this.exception != null) {
        final Throwable e = this.exception;
        this.exception = null;
        return StageSupport.exceptionalStage(e);
      } else {
        return End.endFuture();
      }
    }
  }

  static <T> AsyncIterator<T> errorOnce(final Throwable ex) {
    return new FailOnceAsyncIterator<>(ex);
  }

  static <T> CompletionStage<T> asyncWhileAsyncInitial(
      final Predicate<T> shouldContinue,
      final Function<T, CompletionStage<T>> loopBody,
      final CompletionStage<T> initialValue) {
    return initialValue.thenCompose(t -> AsyncTrampoline.asyncWhile(shouldContinue, loopBody, t));
  }
}
