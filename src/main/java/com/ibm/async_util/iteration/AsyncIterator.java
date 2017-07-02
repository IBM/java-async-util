package com.ibm.async_util.iteration;

import com.ibm.async_util.locks.AsyncLock;
import com.ibm.async_util.locks.FairAsyncLock;
import com.ibm.async_util.util.Either;
import com.ibm.async_util.util.FutureSupport;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A mechanism for asynchronously generating and consuming values
 *
 * <p>Consider this an async version of {@link Stream}.
 *
 * <p>AsyncIterators have lazy, pull based, evaluation semantics - values are not computed until
 * they are needed. AsyncIterators are not immutable - like streams, each value they produce is
 * consumed only once. Typically you should not apply multiple transformations to the same source
 * AsyncIterator, it almost certainly won't do what you want it to do.
 *
 * <p>Implementors of the interface need only implement {@link #nextFuture()}. Alternatively, the
 * static methods {@link #generate(Supplier)}, {@link #supply(Supplier)}, and {@link #unfold(Object,
 * Function)} can be used to create new AsyncIterators from functions that generate iteration
 * elements.
 *
 * <p>A note on thread safety: This class makes no assumption that {@link #nextFuture()} is thread
 * safe! Many methods that generate transformed iterators assume that nextFuture will not be called
 * concurrently, and even stronger, that nextFuture won't be called again until the previous future
 * returned by nextFuture has completed.
 *
 * <p>You can still accomplish parallelization using the ahead methods described below. The
 * difference is that the parallelization in that case is from <b>producing</b> values in parallel,
 * <b>not consuming</b> values in parallel. This means the choice to work in parallel is generally
 * made by the library returning the AsyncIterator, not by the user of it.
 *
 * <p>To implement an AsyncIterator you must only implement the {@link #nextFuture()} method-
 * however, it is recommended that you avoid actually using nextFuture to consume the results of
 * iteration. On top of being tedious, it can also be error prone. It is easy to cause a stack
 * overflow by incorrectly recursing on calls to nextFuture. You should prefer to use the other
 * higher level methods on this interface.
 *
 * <p>There are 2 main categories of such methods on this interface: Intermediate and Terminal.
 * These methods can be combined to form pipelines, which generally consist of a source (often
 * created with the static constructor methods on this interface ({@link #fromIterator(Iterator)},
 * {@link #unfold(Object, Function)}, etc)), followed by zero or more intermediate operations (such
 * as {@link #filter(Predicate)}, {@link #thenApply(Function)}), and completed with a terminal
 * operation which returns a {@link CompletionStage}.
 *
 * <p><b>Intermediate methods</b> - All methods which return {@link AsyncIterator AsyncIterators}
 * are intermediate methods. They can further be broken down into lazy and partially eager methods.
 * Methods that end with the suffix <i>ahead</i> are partially eager, the rest are lazy. A lazy
 * intermediate transformation will not be evaluated until some downstream eager operation is
 * called.
 *
 * <p>Methods ending with the suffix <i> ahead </i>, are partially eager. They can be used when
 * there is an expensive transformation step that should be performed in parallel. They will eagerly
 * consume from their upstream iterator up to a specified amount (still sequentially!) and eagerly
 * apply the transformation step.
 *
 * <p>Intermediate transformations will propagate exceptions similarly to {@link CompletionStage}, a
 * dependent AsyncIterator will return exceptional stages if the upstream iterator generated
 * exceptional elements.
 *
 * <p><b>Terminal operations</b> Methods that return a {@link CompletionStage} instead of another
 * AsyncIterator consume the iterator. After a terminal operation is called, the iterator is
 * considered consumed and should not be used further. If any of the stages in the chain that
 * comprise {@code this} iterator were exceptional, the {@link CompletionStage} returned by a
 * terminal operation will also be exceptional. The exception will short-circuit the terminal
 * operation. For example, a terminal operation such as {@link #forEach(Consumer)} will not to
 * continue to run on subsequent elements of the iterator and instead immediately complete it's
 * returned stage with the error. Unless otherwise noted, this behavior holds for allOf terminal
 * methods but may not documented explicitly.
 *
 * <p>The exception propagation scheme should be familiar to users of {@link CompletionStage},
 * upstream errors will appear wherever the AsyncIterator is consumed and the result is observed
 * (with {@link CompletableFuture#join()} for instance). Exceptions at any stage in the pipeline can
 * be recovered from by using {@link #exceptionally(Function)}, however this won't recover
 * exceptions that are produced downstream. A daring user may have applications where they wish to
 * manually iterate past exceptions without converting them. This is not forbidden, you may use
 * {@link #nextFuture()} directly in which case you are free to continue iterating after an iterator
 * in the chain has produced an exception.
 *
 * <p>The behavior of an AsyncIterator if {@link #nextFuture()} is called after the end of iteration
 * marker is returned is left to the implementation. You may ensure that allOf subsequent calls always
 * return the end marker by using {@link #fuse()}.
 *
 * @param <T> Type of object being iterated over.
 * @see Stream
 */
public interface AsyncIterator<T> {

  /** A marker interface that indicates there are no elements left in the iterator. */
  interface End {}

  /**
   * Returns a future representing the next element of the iterator. This is not a terminal method,
   * it can be safely called multiple times. This method is unique in that it can continue to be
   * called even after a returned stage completes exceptionally. However, this method is not thread
   * safe, and should only be called in a single-threaded fashion. Moreover, after a call another
   * call should not be made until the {@link CompletionStage} returned by the previous call has
   * completed. That is to say,
   *
   * <pre>{@code
   * // illegal
   * pool.exeucte(() -> nextFuture())
   * pool.execute(() -> nextFuture())
   *
   * // just as illegal
   * f1 = nextFuture();
   * f2 = nextFuture();
   *
   * // good
   * nextFuture().thenCompose(t -> nextFuture());
   * }</pre>
   *
   * Though this is not a terminal method, if a terminal method has been called it is no longer safe
   * to call this method. In general this method should be avoided in favor of the higher level
   * methods on this interface unless some unique feature of this method is required. When {@link
   * #nextFuture()} returns {@link End}, the iterator has no more elements. After an iterator emits
   * an {@link End} indicator, the result of subsequent calls to nextFuture is undefined.
   *
   * @return A {@link CompletionStage} of the next element for iteration, or an instance of {@link
   *     End}, indicating the end of iteration.
   */
  CompletionStage<Either<End, T>> nextFuture();

  /**
   * Transforms {@code this} this into a new AsyncIterator that iterates over the results of {@code
   * fn} applied to the outcomes of stages in this iterator when they complete normally.
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString)
   * </pre>
   *
   * returns an AsyncIterator of "1","2","3"...
   *
   * @param fn A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
   *     from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenApply(final Function<? super T, ? extends U> fn) {
    return AsyncIterators.thenApplyImpl(this, fn, null);
  }

  /**
   * Transforms {@code this} into a new AsyncIterator that iterates over the results of {@code fn}
   * applied to the outcomes of stages in this iterator when they complete normally. {@code fn} is
   * executed with the previous stage's default asynchronous execution facility.
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString)
   * </pre>
   *
   * returns an AsyncIterator of "1","2","3"...
   *
   * @param fn A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
   *     from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
    return AsyncIterators.thenApplyImpl(this, fn, ForkJoinPool.commonPool());
  }

  /**
   * Transforms {@code this} into a new AsyncIterator that iterates over the results of {@code fn}
   * applied to the outcomes of stages in this iterator when they complete normally. {@code fn} is
   * executed with the provided Executor.
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString, ex)
   * </pre>
   *
   * returns an AsyncIterator of "1","2","3"...
   *
   * @param fn A function which produces a U from the given T
   * @param executor A {@link Executor} where the function {@code fn} should run
   * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
   *     from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenApplyAsync(
      final Function<? super T, ? extends U> fn, final Executor executor) {
    Objects.requireNonNull(executor);
    return AsyncIterators.thenApplyImpl(this, fn, executor);
  }

  /**
   * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
   * applied to the output from the stages of {@code this}.
   *
   * <pre>{@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString);
   * }</pre>
   *
   * returns an AsyncIterator of "1", "2", "3"...
   *
   * @param fn A function which produces a new CompletionStage from a T
   * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
   *     from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenCompose(final Function<T, CompletionStage<U>> fn) {
    return AsyncIterators.thenComposeImpl(this, fn, null);
  }

  /**
   * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
   * applied to the output from the stages of {@code this}. {@code fn} will be run on the default
   * asynchronous execution facility of the stages of {@code this}.
   *
   * <pre>{@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString);
   * }</pre>
   *
   * returns an AsyncIterator of "1", "2", "3"...
   *
   * @param fn A function which produces a new CompletionStage from a T
   * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
   *     from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenComposeAsync(final Function<T, CompletionStage<U>> fn) {
    return AsyncIterators.thenComposeImpl(this, fn, ForkJoinPool.commonPool());
  }

  /**
   * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
   * applied to the output from the stages of {@code this}. {@code fn} will be run on the supplied
   * executor.
   *
   * <pre>{@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString, executor);
   * }</pre>
   *
   * returns an AsyncIterator of "1", "2", "3"...
   *
   * @param fn A function which produces a new CompletionStage from a T
   * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
   *     from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenComposeAsync(
      final Function<T, CompletionStage<U>> fn, final Executor executor) {
    Objects.requireNonNull(executor);
    return AsyncIterators.thenComposeImpl(this, fn, executor);
  }

  /**
   * Composes fn with the stages of {@code this} iterator to produce new AsyncIterators, and
   * flattens the resulting iterator of iterators.
   *
   * <pre>
   *  AsyncIterator.infiniteRange(1,1) // 1,2,3,...
   *     .thenFlatten(i -> AsyncIterators.range(0, i))
   * </pre>
   *
   * returns an AsyncIterator of {@code 0, 0, 1, 0, 1, 2, 0, 1, 2, 3....}
   *
   * @param fn A function which produces a new AsyncIterator
   * @return A new AsyncIterator consisting of flattened iterators from applying fn to elements of
   *     {@code this}
   */
  default <U> AsyncIterator<U> thenFlatten(final Function<T, AsyncIterator<U>> fn) {
    return AsyncIterator.concat(this.thenApply(fn));
  }

  /**
   * Applies a transformation and flattening to {@code this} iterator with parallelism. This method
   * will consume results from {@code this} sequentially, but will apply the mapping function {@code
   * fn} in parallel. The resulting iterator will retain the order of {@code this}. Up to {@code
   * executeAhead} asynchronous operations past what the consumer of the new iterator has already
   * consumed can be started in parallel.
   *
   * @param fn A function which produces a new AsyncIterator
   * @param executeAhead An integer indicating the number of allowable calls to {@code fn} that can
   *     be made ahead of the user has already consumed
   * @return A new AsyncIterator consisting of flattened iterators from apply {@code fn} to elements
   *     of {@code this}
   * @see #thenFlatten(Function)
   */
  default <U> AsyncIterator<U> thenFlattenAhead(
      final Function<T, AsyncIterator<U>> fn, final int executeAhead) {
    final Function<T, CompletionStage<AsyncIterator<U>>> andThen =
        fn.andThen(CompletableFuture::completedFuture);
    return AsyncIterator.<U>concat(thenComposeAhead(andThen, executeAhead));
  }

  /**
   * Applies a transformation to {@code this} iterator with parallelism. This method will consume
   * results from {@code this} sequentially, but will apply the mapping function {@code f} in
   * parallel. The resulting iterator will retain the order of {@code this}. Up to {@code
   * executeAhead} asynchronous operations past what the consumer of the new iterator has already
   * consumed can be started in parallel.
   *
   * @param f A function which produces a new CompletionStage
   * @param executeAhead An integer indicating the number of allowable calls to f that can be made
   *     ahead of the user has already consumed
   * @return A transformed AsyncIterator
   * @see #thenCompose(Function)
   */
  default <U> AsyncIterator<U> thenComposeAhead(
      final Function<T, CompletionStage<U>> f, final int executeAhead) {
    // apply user function and wrap future result in a Option
    final Function<Either<End, T>, CompletionStage<Either<End, U>>> eitherF =
        nt -> {
          return nt.fold(
              stop -> AsyncIterators.endFuture(), t -> f.apply(t).thenApply(Either::right));
        };

    // allOf queue modifications happen under the async lock
    return new AsyncIterator<U>() {
      final Queue<CompletionStage<Either<End, U>>> pendingResults = new ArrayDeque<>(executeAhead);
      final FairAsyncLock lock = new FairAsyncLock();

      /* return whether we need to keep filling */
      private CompletionStage<Either<End, T>> fillMore() {
        if (pendingResults.size() >= executeAhead) {
          // don't call nextFuture, we already have enough stuff pending
          return AsyncIterators.endFuture();
        } else {
          // keep filling up the ahead queue
          CompletionStage<Either<End, T>> nxt =
              AsyncIterators.convertSynchronousException(AsyncIterator.this::nextFuture);
          pendingResults.add(nxt.thenCompose(eitherF));
          return nxt;
        }
      }

      /* find someone for listener to listen to, kickoff a call to fill the queue */
      private CompletionStage<Either<End, T>> attachListener(
          final CompletableFuture<Either<End, U>> listener) {
        final CompletionStage<Either<End, U>> poll = pendingResults.poll();
        if (poll == null) {

          // there was nothing in the queue, associate our returned future with a new
          // safeNextFuture call
          CompletionStage<Either<End, T>> nxt =
              AsyncIterators.convertSynchronousException(AsyncIterator.this::nextFuture);

          // don't bother adding it to the queue, because we are already listening on it
          AsyncIterators.listen(nxt.thenCompose(eitherF), listener);

          return nxt;
        } else {
          // let our future be tied to the first result that was in the queue
          AsyncIterators.listen(poll, listener);
          return fillMore();
        }
      }

      @Override
      public CompletionStage<Either<End, U>> nextFuture() {

        final CompletableFuture<Either<End, U>> retFuture = new CompletableFuture<>();

        // whether this simple future already has a buddy in the pendingResults queue
        // just need a local final reference, other things enforce memory barriers
        final boolean[] connected = new boolean[1];
        connected[0] = false;

        AsyncTrampoline.asyncWhile(
            () -> {
              CompletionStage<AsyncLock.LockToken> lockFuture = lock.acquireLock();
              return lockFuture.thenCompose(
                  token -> {
                    CompletionStage<Either<End, T>> next;
                    if (connected[0]) {
                      next = fillMore();
                    } else {
                      connected[0] = true;
                      // find something for retFuture to listen to
                      next = attachListener(retFuture);
                    }
                    return next.thenApply(Either::isRight)
                        // exceptional futures get added to the queue same as normal ones,
                        // we may continue filling
                        .exceptionally(e -> true)
                        .whenComplete((t, ex) -> token.releaseLock());
                  });
            });
        return retFuture;
      }
    };
  }

  /**
   * Transforms the AsyncIterator into one which will only produce results that match {@code
   * predicate}.
   *
   * @param predicate A function that takes a T and returns true if it should be returned by the new
   *     iterator, and false otherwise
   * @return a new AsyncIterator which will only return results that match predicate
   */
  default AsyncIterator<T> filter(final Predicate<T> predicate) {

    // keep looping looking for a value that satisfies predicate as long as the current value
    // doesn't, and we're not out of elements
    final Predicate<Either<End, T>> shouldKeepLooking =
        either -> either.fold(end -> false, predicate.negate()::test);

    return () ->
        AsyncIterator.this
            .nextFuture()
            .thenCompose(
                t ->
                    AsyncTrampoline.asyncWhile(
                        shouldKeepLooking, c -> AsyncIterator.this.nextFuture(), t));
  }

  /**
   * Applies a transformation and filters this AsyncIterator at the same time. Since {@link
   * Optional} cannot hold a null values, this method cannot be used to map to an iterator of
   * possibly null types.
   *
   * @param fn a conditional transformation from T to U. If fn produces empty, this result will not
   *     be included in the new iterator
   * @return An AsyncIterator of allOf the Us that were present
   */
  default <U> AsyncIterator<U> filterApply(final Function<T, Optional<U>> fn) {
    return this.thenApply(fn).filter(Optional::isPresent).thenApply(Optional::get);
  }

  /**
   * Composes and filters an AsyncIterator at the same time. Since {@link Optional} cannot hold a
   * null values, this method cannot be used to map to an iterator of possibly null types.
   *
   * @param fn an asynchronous conditional transformation from T to U. If fn produces empty, this
   *     result will not be included in the new iterator
   * @return An AsyncIterator of allOf the Us that were present
   */
  default <U> AsyncIterator<U> filterCompose(final Function<T, CompletionStage<Optional<U>>> fn) {
    return this.thenCompose(fn).filter(Optional::isPresent).thenApply(Optional::get);
  }

  /**
   * Returns an AsyncIterator that will return only the first n elements of {@code this}
   * AsyncIterator.
   *
   * @param n the maximum number of elements to take from this iterator
   * @return an AsyncIterator which will return n elements or less.
   */
  default AsyncIterator<T> take(final long n) {
    return new AsyncIterator<T>() {
      int count = 0;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (++count > n) {
          return AsyncIterators.endFuture();
        } else {
          return AsyncIterator.this.nextFuture();
        }
      }
    };
  }

  /**
   * Returns an AsyncIterator that returns elements from the backing iterator until coming across an
   * element that does not satisfy the predicate.
   *
   * @param predicate a predicate which returns {@code true} if we can continue returning values
   *     from the iterator, and {@code false otherwise}
   * @return A new AsyncIterator that will return T's until the predicate fails
   */
  default AsyncIterator<T> takeWhile(final Predicate<T> predicate) {
    return new AsyncIterator<T>() {
      boolean predicateFailed = false;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return AsyncIterator.this
            .nextFuture()
            .thenApply(
                either ->
                    either.flatMap(
                        t -> {
                          if (predicateFailed) {
                            return AsyncIterators.end();
                          } else if (!predicate.test(t)) {
                            predicateFailed = true;
                            return AsyncIterators.end();
                          } else {
                            return Either.right(t);
                          }
                        }));
      }
    };
  }

  /**
   * Returns an AsyncIterator where any exception produced by {@code this} iterator will be
   * transformed with the provided function.
   *
   * @param fn the Function used to convert an error from this iterator into a T. If {@code fn}
   *     itself throws an exception, that exception will be emitted in the resulting iterator.
   * @return a new AsyncIterator where exceptions from this iterator have been converted using
   *     {@code fn}
   */
  default AsyncIterator<T> exceptionally(Function<Throwable, T> fn) {
    return () ->
        AsyncIterators.convertSynchronousException(this::nextFuture)
            .exceptionally(ex -> Either.right(fn.apply(ex)));
  }

  /**
   * Fuses the iterator to the {@link End} result after iteration has stopped. It is normally
   * undefined behavior to call {@link #nextFuture()} after {@link End} has already been returned.
   * On a fused iterator, nextFuture will just continue to return End.
   *
   * @return An iterator where it is safe to call {@link #nextFuture()} after {@link End} has
   *     already been returned
   */
  default AsyncIterator<T> fuse() {
    return new AsyncIterator<T>() {
      boolean end = false;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (end) {
          return AsyncIterators.endFuture();
        }
        return AsyncIterator.this
            .nextFuture()
            .thenApply(
                either -> {
                  either.forEach(endMarker -> end = true, t -> {});
                  return either;
                });
      }
    };
  }

  /**
   * Collect the results of this iterator in batches, returning an iterator of those batched
   * collections.
   *
   * <p>This may be useful for performing bulk operations on many elements, rather than on one
   * element at a time.
   *
   * @param collector a {@link Collector} used to collect the elements of this iterator into
   *     individual batches. Each batch will be created by invoking the collector's {@link
   *     Collector#supplier()} method
   * @param shouldAddToBatch a predicate which determines whether a given element encountered during
   *     iteration should be added to the given (current) batch. If this predicate returns true for
   *     the given element and container, the element will be {@link Collector#accumulator() added}
   *     to the container, and the batching operation will continue to draw from the underlying
   *     iterator. If this predicate returns false, the element will not be added and the current
   *     batch will be {@link Collector#finisher() finished} and returned by the batching iterator.
   *     The element which did not meet the predicate will be tested again by the next batch
   * @return an AsyncIterator which invokes several iterations of the underlying iterator with each
   *     advance, collecting these elements into containers provided by the given {@link Collector}.
   */
  default <A, R> AsyncIterator<R> batch(
      final Collector<? super T, A, R> collector,
      final BiPredicate<? super A, ? super T> shouldAddToBatch) {
    return new AsyncIterator<R>() {
      /**
       * This field holds the result of the latest call to the underlying iterator's 'nextFuture';
       * At the start of the batching iterator's 'nextFuture' method, this holds the value which was
       * rejected by the last 'addToBatch' call (or empty if the iterator terminated, or null if
       * this is the first call). If non-empty, this rejected value should be tested again in the
       * next batch. If empty, iteration should terminate
       */
      private Either<End, T> lastAdvance = null;

      @Override
      public CompletionStage<Either<End, R>> nextFuture() {
        // the first call has no preceding value to start the batch, so draw from iter
        return this.lastAdvance == null
            ? AsyncIterator.this
                .nextFuture()
                .thenCompose(
                    eitherT -> {
                      this.lastAdvance = eitherT;
                      return collectBatch();
                    })
            : collectBatch();
      }

      private CompletionStage<Either<End, R>> collectBatch() {
        return this.lastAdvance.fold(
            end -> AsyncIterators.endFuture(),
            ignoredT -> {
              final A batch = collector.supplier().get();

              return AsyncTrampoline.asyncWhile(
                      eitherT ->
                          eitherT.right().filter(t -> shouldAddToBatch.test(batch, t)).isPresent(),
                      eitherT -> {
                        collector
                            .accumulator()
                            .accept(batch, eitherT.right().orElseThrow(IllegalStateException::new));
                        return AsyncIterator.this.nextFuture();
                      },
                      this.lastAdvance)
                  .thenApply(
                      eitherT -> {
                        this.lastAdvance = eitherT;
                        return Either.right(AsyncIterators.finishContainer(batch, collector));
                      });
            });
      }
    };
  }

  /**
   * A convenience method provided to invoke {@link #batch(Collector, BiPredicate)} with a predicate
   * that limits batches to a fixed size.
   *
   * <p>Each batch will be as large as the given {@code batchSize} except possibly the last one,
   * which may be smaller due to exhausting the underlying iterator.
   *
   * @see #batch(Collector, BiPredicate)
   */
  default <A, R> AsyncIterator<R> batch(
      final Collector<? super T, A, R> collector, final int batchSize) {
    class CountingContainer {
      final A container;
      int size;

      public CountingContainer(final A container, final int size) {
        this.container = container;
        this.size = size;
      }
    }

    class CountingCollector
        implements Collector<T, CountingContainer, R>,
            Supplier<CountingContainer>,
            BiConsumer<CountingContainer, T>,
            BinaryOperator<CountingContainer>,
            BiPredicate<CountingContainer, T> {
      private final Supplier<A> parentSupplier = collector.supplier();
      private final BiConsumer<A, ? super T> parentAccumulator = collector.accumulator();
      private final BinaryOperator<A> parentCombiner = collector.combiner();
      private final Set<Collector.Characteristics> characteristics;

      public CountingCollector() {
        final Set<Collector.Characteristics> characteristics =
            EnumSet.copyOf(collector.characteristics());
        // remove concurrent (if present) because the increments aren't thread safe
        characteristics.remove(Characteristics.CONCURRENT);

        // remove identity (if present) because the finisher is necessary to unbox the container
        characteristics.remove(Characteristics.IDENTITY_FINISH);
        this.characteristics = Collections.unmodifiableSet(characteristics);
      }

      @Override
      public Supplier<CountingContainer> supplier() {
        return this;
      }

      @Override
      public BiConsumer<CountingContainer, T> accumulator() {
        return this;
      }

      @Override
      public BinaryOperator<CountingContainer> combiner() {
        return this;
      }

      @Override
      public Function<CountingContainer, R> finisher() {
        return countingContainer ->
            AsyncIterators.finishContainer(countingContainer.container, collector);
      }

      @Override
      public Set<Collector.Characteristics> characteristics() {
        return this.characteristics;
      }

      // supplier
      @Override
      public CountingContainer get() {
        return new CountingContainer(this.parentSupplier.get(), 0);
      }

      // accumulator
      @Override
      public void accept(final CountingContainer countingContainer, final T t) {
        this.parentAccumulator.accept(countingContainer.container, t);
        countingContainer.size++;
      }

      // combiner
      @Override
      public CountingContainer apply(final CountingContainer c1, final CountingContainer c2) {
        final A combined = this.parentCombiner.apply(c1.container, c2.container);
        // many mutable collectors simply addAll to the left container and return it.
        // this is an optimistic check to save a new container creation
        if (combined == c1.container) {
          c1.size += c2.size;
          return c1;
        } else {
          return new CountingContainer(combined, c1.size + c2.size);
        }
      }

      // shouldAddToBatch
      @Override
      public boolean test(final CountingContainer countingContainer, final T t) {
        return countingContainer.size < batchSize;
      }
    }

    final CountingCollector counter = new CountingCollector();
    return batch(counter, counter);
  }

  /**
   * Sequentially accumulates the elements of type T in this iterator into a U. This provides an
   * immutable style terminal reduction operation as opposed to the mutable style supported by
   * {@link #collect}. For example, to sum the lengths of Strings in an AsyncIterator, {@code
   * stringIt.fold((acc, s) -> acc + s.length(), 0)}.
   *
   * @param accumulator a function that produces a new accumulation from an existing accumulation
   *     and a new element
   * @param identity a starting U value
   * @return a {@link CompletionStage} containing the resulting U from repeated application of
   *     accumulator
   */
  default <U> CompletionStage<U> fold(final BiFunction<U, T, U> accumulator, final U identity) {
    @SuppressWarnings("unchecked")
    U[] uarr = (U[]) new Object[] {identity};
    return this.collect(() -> uarr, (u, t) -> uarr[0] = accumulator.apply(uarr[0], t))
        .thenApply(arr -> arr[0]);
  }

  /**
   * Sequentially accumulates the elements of type T in this iterator into a single T value. This
   * provides an immutable style terminal reduction operation as opposed to the mutable style
   * supported by {@link #collect}. For example, to sum an iterator of ints, {@code intIt.fold((acc,
   * i) -> acc + i, 0)}.
   *
   * @param accumulator a function that takes the current accumulated value and a value to fold in
   *     (in that order), and produces a new accumulated value.
   * @param identity a default T value
   * @return a {@link CompletionStage} containing the resulting T from repeated application of
   *     accumulator
   */
  default CompletionStage<T> fold(final BinaryOperator<T> accumulator, final T identity) {

    // don't make this a lambda - otherwise it will look like a BinaryOperator instead of a
    // BiFunction and we'll recurse
    final BiFunction<T, T, T> biAccumulator =
        new BiFunction<T, T, T>() {
          @Override
          public T apply(final T t, final T u) {
            return accumulator.apply(t, u);
          }
        };
    return fold(biAccumulator, identity);
  }

  /**
   * Force the eager evaluation of the entire iterator, stopping only when {@code this} iterator is
   * out of elements or an exception is encountered.
   *
   * @return a {@link CompletionStage} that is completed when consumption is finished
   */
  default CompletionStage<Void> consume() {
    return AsyncTrampoline.asyncWhile(() -> nextFuture().thenApply(Either::isRight));
  }

  /**
   * Perform a mutable reduction operation using collector and return a CompletionStage of the
   * result.
   *
   * @param collector a {@link Collector} which will sequentially collect the contents of this
   *     iterator into an {@code R}
   * @param <A> The intermediate type of the accumulated
   * @param <R> The final type of the accumulated object
   * @return a {@link CompletionStage} which will complete with the collected value
   * @see Stream#collect(Collector)
   */
  default <R, A> CompletionStage<R> collect(final Collector<? super T, A, R> collector) {
    final A container = collector.supplier().get();
    final BiConsumer<A, ? super T> acc = collector.accumulator();
    return forEach(t -> acc.accept(container, t))
        .thenApply(ig -> AsyncIterators.finishContainer(container, collector));
  }

  /**
   * Perform a mutable reduction operation and return a {@link CompletionStage} of the result. A
   * mutable reduction is one where the accumulator has mutable state and additional elements are
   * incorporated by updating that state.
   *
   * @param supplier a supplier for a stateful accumulator
   * @param accumulator a function which can incorporate T elements into a stateful accumulation
   * @return a {@link CompletionStage} which will complete with the accumulated value
   * @see Stream#collect(Supplier, BiConsumer, BiConsumer)
   */
  default <R> CompletionStage<R> collect(
      final Supplier<R> supplier, final BiConsumer<R, ? super T> accumulator) {
    final R container = supplier.get();
    return forEach(t -> accumulator.accept(container, t)).thenApply(ig -> container);
  }

  /**
   * Performs the side effecting action until the end of iteration is reached
   *
   * @param action a side-effecting action that takes a T
   * @return a {@link CompletionStage} that returns when there are no elements left to apply action
   *     to, or an exception has been encountered.
   */
  default CompletionStage<Void> forEach(final Consumer<T> action) {
    return AsyncTrampoline.asyncWhile(
        () ->
            nextFuture()
                .thenApply(
                    eitherT -> {
                      eitherT.forEach(ig -> {}, action);
                      return eitherT.isRight();
                    }));
  }

  /**
   * Gets the first element that satisfies predicate, or empty if no such element exists
   *
   * @param predicate the predicate that returns true for the desired element
   * @return a {@link CompletionStage} that completes with the first T to satisfy predicate, or
   *     empty if no such T exists
   */
  default CompletionStage<Optional<T>> find(final Predicate<T> predicate) {
    return this.filter(predicate).nextFuture().thenApply(Either::<End, T>right);
  }

  /**
   * Flatten a collection of AsyncIterators into a single AsyncIterator. If the collection has a
   * defined order, that order is preserved in the concatenated result.
   *
   * <pre>{@code
   * // returns an AsyncInterator of 0,1,2,3,4
   * AsyncIterators.concat(Arrays.asList(
   *   AsyncIterators.range(0, 3, 1),
   *   AsyncIterators.range(3, 5, 1)))
   * }</pre>
   *
   * @param asyncIterators an Collection of AsyncIterators to concatenate
   * @return A single AsyncIterator that is the concatenation of asyncIterators
   */
  static <T> AsyncIterator<T> concat(final Collection<AsyncIterator<T>> asyncIterators) {
    if (asyncIterators.isEmpty()) {
      return AsyncIterator.empty();
    }
    final Queue<AsyncIterator<T>> q = new ArrayDeque<>(asyncIterators);

    return supply(
        () ->
            q.peek()
                .nextFuture()
                .thenCompose(
                    first -> {
                      return AsyncTrampoline.<Either<End, T>>asyncWhile(
                          nt -> !nt.isRight() && q.poll() != null && !q.isEmpty(),
                          ig -> q.peek().nextFuture(),
                          first);
                    }));
  }

  /**
   * Flatten an AsyncIterator of AsyncIterators into a single AsyncIterator
   *
   * <pre>{@code
   * // returns an AsyncInterator of 0,1,2,0,1,2,0,1,2
   * AsyncIterators.concat(AsyncIterators.generate(() -> AsyncIterators.range(0, 3, 1)).take(3))
   * }</pre>
   *
   * @param asyncIterators a AsyncIterator of AsyncIterators
   * @return A single AsyncIterator that is the concatenation of asyncIterators
   */
  static <T> AsyncIterator<T> concat(final AsyncIterator<AsyncIterator<T>> asyncIterators) {
    return new AsyncIterator<T>() {
      // initially set the iterator to empty so we always recurse the first time
      // after that, if curr ever becomes null, we're done
      AsyncIterator<T> curr = AsyncIterator.empty();

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (this.curr == null) {
          // out of iterators
          return AsyncIterators.endFuture();
        }

        /*
         * on each iteration call nextFuture. If it's empty, we should set curr to the next
         * iterator. If we are out of iterators, curr becomes null, and we stop iterating
         */
        return AsyncTrampoline.<Either<End, T>>asyncWhile(
            ot -> !ot.isRight() && this.curr != null,
            ot -> {
              return this.curr
                  .nextFuture()
                  .thenCompose(
                      either -> {
                        /*
                         * if the result from the last call to nextFuture() is empty, grab another
                         * AsyncIterator out of asyncIterators and set curr
                         */
                        return either.fold(
                            end -> {
                              // current iterator was out of elements
                              return asyncIterators
                                  .nextFuture()
                                  .thenApply(
                                      nextIt -> {
                                        // null if no iterators left
                                        this.curr = nextIt.right().orElse(null);
                                        // now curr has been updated so a retry will use
                                        // the next iterator if it exists
                                        return AsyncIterators.end();
                                      });
                            },
                            t ->
                                (CompletionStage<Either<End, T>>)
                                    CompletableFuture.completedFuture(Either.<End, T>right(t)));
                      });
            },
            // initially pass empty, so we always enter the loop
            AsyncIterators.end());
      }
    };
  }

  /**
   * Create an iterator that is the result of fn applied to iteration elements returned by tIt and
   * uI. If either input iterator terminates, the returned iterator will terminate. If either input
   * iterator returns an exception, an exceptional result will be emitted by the returned iterator.
   * In this case, a single result will still be consumed from both iterators.
   *
   * @param tIt an AsyncIterator of Ts
   * @param uIt an AsyncIterator of Us
   * @param fn a function that produces a V from a T and a U
   * @return AsyncIterator of fn applied to elements of tIt and uIt
   */
  static <T, U, V> AsyncIterator<V> zipWith(
      final AsyncIterator<T> tIt, final AsyncIterator<U> uIt, final BiFunction<T, U, V> fn) {
    // once allOf futures are complete, if allOf are nonempty, then apply fn to the arg
    return () -> {
      // call nextFuture before checking for an exception
      CompletionStage<Either<End, T>> tFuture =
          AsyncIterators.convertSynchronousException(tIt::nextFuture);
      CompletionStage<Either<End, U>> uFuture =
          AsyncIterators.convertSynchronousException(uIt::nextFuture);
      return tFuture.thenCombine(uFuture, (et, eu) -> AsyncIterators.zipWith(et, eu, fn));
    };
  }

  /**
   * Creates an empty AsyncIterator.
   *
   * @return an AsyncIterator that will immediately produce an {@link End} marker
   */
  @SuppressWarnings("unchecked")
  static <T> AsyncIterator<T> empty() {
    return (AsyncIterator<T>) AsyncIterators.EMPTY_ITERATOR;
  }

  /**
   * Creates an AsyncIterator from an {@link Iterator}
   *
   * @param iterator an {@link Iterator} of T elements
   * @return A new AsyncIterator which will yield the elements of {@code iterator}
   */
  static <T> AsyncIterator<T> fromIterator(final Iterator<T> iterator) {
    return () ->
        CompletableFuture.completedFuture(
            iterator.hasNext() ? Either.right(iterator.next()) : AsyncIterators.end());
  }

  /**
   * Creates an AsyncIterator from a {@link CompletionStage} of an {@link Iterator}.
   *
   * @param stage a {@link CompletionStage} stage that produces an AsyncIterator
   * @return a new AsyncIterator which will yield the elements of the produced iterator
   */
  static <T> AsyncIterator<T> fromIteratorStage(final CompletionStage<AsyncIterator<T>> stage) {
    return () -> stage.thenCompose(AsyncIterator::nextFuture);
  }

  /**
   * Creates an AsyncIterator of one element.
   *
   * @param t the element to return
   * @return an AsyncIterator which yield the element t, and then afterward produce the {@link End}
   *     marker.
   */
  static <T> AsyncIterator<T> once(final T t) {
    return new AsyncIterator<T>() {
      Either<End, T> curr = Either.right(t);

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        Either<End, T> prev = curr;
        curr = AsyncIterators.end();
        return CompletableFuture.completedFuture(prev);
      }
    };
  }

  /**
   * Creates an AsyncIterator for which allOf downstream operations will be completed with an
   * exception.
   *
   * @param ex the exception which the {@link CompletionStage CompletionStages} of the returned
   *     future will be completed with
   * @return an AsyncIterator that produces exceptional CompletionStages
   */
  static <T> AsyncIterator<T> error(final Throwable ex) {
    CompletableFuture<Either<End, T>> future = new CompletableFuture<>();
    future.completeExceptionally(ex);
    return supply(() -> future);
  }

  /**
   * Creates an infinite AsyncIterator of the same value.
   *
   * @param t the value to repeat
   * @return An AsyncIterator that will always return {@code t}
   */
  static <T> AsyncIterator<T> repeat(final T t) {
    final Either<End, T> ret = Either.right(t);
    return supply(() -> CompletableFuture.completedFuture(ret));
  }

  /**
   * Create an AsyncIterator for a range.
   *
   * <p>If delta is positive, similar to {@code for(i = start; start < end; start+=delta)}. If delta
   * is negative, similar to {@code for(i = start; start > end; start+=delta)}.
   *
   * <p>The futures returned by nextFuture will be already completed.
   *
   * @param start the start point of iteration (inclusive)
   * @param end the end point of iteration (exclusive)
   * @param delta the step amount for each iterator, it may be negative
   * @return an AsyncIterator that will return a integers from start to end incremented by delta
   */
  static AsyncIterator<Integer> range(final int start, final int end, final int delta) {
    if (delta == 0) {
      throw new IllegalArgumentException("increment/decrement must be nonzero");
    }
    return new AsyncIterator<Integer>() {
      int counter = start;

      @Override
      public CompletionStage<Either<End, Integer>> nextFuture() {
        if ((delta > 0 && counter < end) || (delta < 0 && counter > end)) {
          int ret = counter;
          counter += delta;
          return CompletableFuture.completedFuture(Either.right(ret));
        } else {
          return AsyncIterators.endFuture();
        }
      }
    };
  }

  /**
   * Create an infinite AsyncIterator for a range.
   *
   * <p>If you try to consume this entire iterator (perhaps by using a 'terminal' operation like
   * {@link AsyncIterator#fold}), it will infinite loop.
   *
   * <p>The futures returned by nextFuture will be already completed.
   *
   * @param start the start point of iteration (inclusive)
   * @param delta the increment/decrement for each iteration (may be negative)
   * @return an AsyncIterator that will return a integers starting with start incremented by delta
   */
  static AsyncIterator<Integer> infiniteRange(final int start, final int delta) {
    if (delta == 0) {
      throw new IllegalArgumentException("increment/decrement must be nonzero");
    }
    return new AsyncIterator<Integer>() {
      int counter = start;

      @Override
      public CompletionStage<Either<End, Integer>> nextFuture() {
        int old = counter;
        counter += delta;
        return CompletableFuture.completedFuture(Either.right(old));
      }
    };
  }

  /**
   * Create an AsyncIterator from a collection of future. As each future completes, the returned
   * iterator yields a value. As the name implies, the order in which values are returned does not
   * reflect the original order of the collection of futures.
   *
   * @param futures a Collection of futures that will be emitted in the returned iterator as they
   *     complete
   * @return AsyncIterator of values produced by futures in order of completion
   */
  static <T> AsyncIterator<T> unordered(final Collection<? extends CompletionStage<T>> futures) {
    final AtomicInteger size = new AtomicInteger(futures.size());
    final AsyncChannel<Either<Throwable, T>> channel = AsyncChannels.unbounded();
    for (CompletionStage<T> future : futures) {
      future.whenComplete(
          (t, ex) -> {
            Either<Throwable, T> toSend = t != null ? Either.right(t) : Either.left(ex);
            channel.send(toSend);
            if (size.decrementAndGet() == 0) {
              // terminate the channel
              channel.terminate();
            }
          });
    }
    return channel.thenCompose(
        either -> either.fold(FutureSupport::errorStage, CompletableFuture::completedFuture));
  }

  /**
   * Creates an infinite AsyncIterator of type T.
   *
   * @param supplier supplies stages for elements to be yielded by the returned iterator
   * @return AsyncIterator returning values generated from {@code supplier}
   */
  static <T> AsyncIterator<T> generate(final Supplier<CompletionStage<T>> supplier) {
    return () -> supplier.get().thenApply(Either::right);
  }

  /**
   * Creates an AsyncIterator of type T
   *
   * @param supplier produces CompletionStages of elements in the iterator or indicates end of
   *     iteration with {@link End}
   * @return An AsyncIterator that produces the values generated by the {@code supplier}
   */
  static <T> AsyncIterator<T> supply(final Supplier<CompletionStage<Either<End, T>>> supplier) {
    return supplier::get;
  }

  /**
   * Successively apply the asynchronous function f to the seed until the future returned by f
   * contains an empty optional or returns an exception. Creates an iterator of values of
   * applications
   *
   * <p>For example, if {@code f = t -> CompletableFuture.completedFuture(Either.right(f(t)))}, then
   * this would produce an asynchronous stream of the values {@code seed, f(seed), f(f(seed)),
   * f(f(f(seed))),...}. The iterator is potentially infinite - it would be in the preceding
   * example.
   *
   * @param seed the first value produced by the returned iterator
   * @param f the function that is applied to the previous value to generate the next value
   * @return AsyncIterator of the values returned by f
   * @see Stream#iterate(Object, java.util.function.UnaryOperator)
   */
  static <T> AsyncIterator<T> unfold(
      final T seed, final Function<T, CompletionStage<Either<End, T>>> f) {
    return new AsyncIterator<T>() {
      CompletionStage<Either<End, T>> prev = CompletableFuture.completedFuture(Either.right(seed));

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        // if there was a value, apply f to it
        CompletionStage<Either<End, T>> ret = prev;
        this.prev = this.prev.thenCompose(nxt -> nxt.fold(end -> AsyncIterators.endFuture(), f));
        return ret;
      }
    };
  }
}
