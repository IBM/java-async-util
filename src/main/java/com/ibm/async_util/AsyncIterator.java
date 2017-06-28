package com.ibm.async_util;

import com.ibm.async_util.AsyncLock.LockToken;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
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
 * <p>Though this class is abstract, it cannot be extended by users. Instead use the static methods
 * {@link #generate(Supplier)}, {@link #supply(Supplier)}, and {@link #unfold(Object, Function)} to
 * create new AsyncIterators.
 *
 * <p>A note on thread safety: This class makes no assumption that nextFuture is thread safe! Many
 * methods that generate transformed iterators assume that nextFuture will not be called
 * concurrently, and even stronger, that nextFuture won't be called again until the previous future
 * returned by nextFuture has completed. That is to say,
 *
 * <pre>{@code
 * // bad
 * pool.exeucte(() -> nextFuture())
 * pool.execute(() -> nextFuture())
 *
 * // just as bad
 * f1 = nextFuture();
 * f2 = nextFuture();
 *
 * // good
 * nextFuture().thenCompose(t -> nextFuture());
 * }</pre>
 *
 * You can still accomplish parallelization using the ahead methods described below. The difference
 * is that the parallelization in that case is from <b>producing</b> values in parallel, <b>not
 * consuming</b> values in parallel. This means the choice to work in parallel is generally made by
 * the library returning the AsyncIterator, not by the user of it.
 *
 * <p>To implement an AsyncIterator you must only implement the {@link #nextFuture()} method-
 * however, it is recommended that you avoid actually using nextFuture to consume the results of
 * iteration. On top of being tedious, it can also be error prone. It is easy to cause a stack
 * overflow by incorrectly recursing on calls to nextFuture. You should prefer to use the other
 * higher level methods on this interface.
 *
 * <p>There are 3 categories of method on this interface
 *
 * <ol>
 *   <li>Lazy transformations - every method that returns another AsyncIterator is truly lazy except
 *       for those in (3). This means that no calls to nextFuture on {@code this} will occur until a
 *       call to nextFuture on the transformed iterator occurs. For example,
 *       <pre>{@code
 * AsyncIterator<Integer> myIt = AsyncIterator.generate(() -> {
 *     System.out.println("hello");
 *     return 1;
 * });
 * transformed = myIt.thenApply(t -> i + 1);
 * }</pre>
 *       will not print hello, because the mapping was lazy. However the moment one consumes a value
 *       from transformed, i.e. {@code transformed.nextFuture();}, hello will be printed. Lazy
 *       transformations will propagate exceptions similarly to {@link CompletionStage}, a dependent
 *       AsyncIterator will return exceptional futures if the upstream iterator generated
 *       exceptional elements.
 *   <li>Terminal/consumption operations. Methods that return a {@link CompletionStage} instead of
 *       another AsyncIterator consume all or part of the iterator. If any of the stages in the
 *       chain that comprise {@code this} iterator were exceptional, the {@link CompletionStage}
 *       returned by a terminal operation will also be exceptional.
 *   <li>Parallel. Methods that end with 'ahead' will allow you to perform an expensive
 *       transformation step in parallel. this iterator will still be consumed in a sequential
 *       fashion, but this consumption can happen before downstream operations have consumed it. In
 *       the example from (1), if it were changed to
 *       <pre>{@code
 * transformed = myIt.thenComposeAhead(t -> CompletableFuture.completedFuture(i + 1), 5);
 * transformed.nextFuture();
 * }</pre>
 *       you would expect to see a hello printed immediately multiple times with the ahead call,
 *       even though nextFuture was only called once.
 * </ol>
 *
 * <p>The exception propagation scheme should be familiar to users of {@link CompletionStage},
 * upstream errors will appear wherever the AsyncIterator is consumed and the result is observed
 * (with {@link CompletableFuture#join()} for instance). That being said, a daring user may have
 * applications where they wish to recover from exceptions and continue iteration. This is not
 * forbidden, you may use {@link #nextFuture()} directly in which case you are free to continue
 * iterating after an iterator in the chain has produced an exception.
 *
 * <p>The behavior of an AsyncIterator if {@link #nextFuture()} is called after the end of iteration
 * marker is returned is left to the implementation. You may ensure that all subsequent calls always
 * return the end marker by using {@link #fuse()}.
 *
 * @param <T> Type of object being iterated over.
 * @see Stream
 */
public abstract class AsyncIterator<T> {

  AsyncIterator() {}

  /** A marker interface that indicates there are no elements left in the iterator. */
  public interface End {}

  /**
   * Returns a future representing the next element of the iterator.
   *
   * <p>If the optional is empty, the iterator has no more elements. After an iterator returns
   * empty, a consumer should make no subsequent calls to nextFuture
   *
   * @return A future of the next element for iteration, or empty
   */
  public abstract CompletionStage<Either<End, T>> nextFuture();

  private CompletionStage<Either<End, T>> safeNextFuture() {
    try {
      return this.nextFuture();
    } catch (Throwable e) {
      return AsyncIterators.exceptional(e);
    }
  }

  /**
   * Applies f to the results of the stages in {@code this} iterator
   *
   * <p>Example:
   *
   * <p>
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString)
   * </pre>
   *
   * returns an AsyncIterator of "1","2","3"...
   *
   * @param f A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of f applied to the result of the stages from
   *     {@code this} iterator
   */
  public <U> AsyncIterator<U> thenApply(final Function<? super T, ? extends U> f) {
    return supply(() -> safeNextFuture().thenApply(either -> either.map(f)));
  }

  /**
   * Applies f to the results of the stages in {@code this} iterator, executed with the previous
   * stage's default asynchronous execution facility.
   *
   * <p>Example:
   *
   * <p>
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString)
   * </pre>
   *
   * returns an AsyncIterator of "1","2","3"...
   *
   * @param f A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of f applied to the result of the stages from
   *     {@code this} iterator
   */
  public <U> AsyncIterator<U> thenApplyAsync(final Function<? super T, ? extends U> f) {
    return supply(() -> safeNextFuture().thenApplyAsync(either -> either.map(f)));
  }

  /**
   * Applies f to the results of the stages in {@code this} iterator, executed with the provided
   * Executor.
   *
   * <p>Example:
   *
   * <p>
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString, ex)
   * </pre>
   *
   * returns an AsyncIterator of "1","2","3"...
   *
   * @param f A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of f applied to the result of the stages from
   *     {@code this} iterator
   */
  public <U> AsyncIterator<U> thenApplyAsync(
      final Function<? super T, ? extends U> f, final Executor executor) {
    return supply(() -> safeNextFuture().thenApplyAsync(either -> either.map(f), executor));
  }

  /**
   * Creates a new AsyncIterator using the produced stages of {@code f} applied to the output from
   * the stages of {@code this}.
   *
   * <pre>{@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString);
   * }</pre>
   *
   * returns an AsyncIterator of "1", "2", "3"...
   *
   * @param f A function which produces a new CompletionStage from a T
   * @return A new AsyncIterator which produces stages of f composed with the result of the stages
   *     from {@code this} iterator
   */
  public <U> AsyncIterator<U> thenCompose(final Function<T, CompletionStage<U>> f) {
    return supply(
        () ->
            safeNextFuture()
                .thenCompose(
                    nt -> {
                      // if there's a value, apply f and wrap the result in an Either,
                      // otherwise just return end marker
                      return nt.fold(
                          end -> AsyncIterators.endFuture(),
                          t -> f.apply(t).thenApply(Either::right));
                    }));
  }

  /**
   * Creates a new AsyncIterator using the produced stages of {@code f} applied to the output from
   * the stages of {@code this}. {@code f} will be run on the default asynchronous execution
   * facility of the stages of {@code this}.
   *
   * <pre>{@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString);
   * }</pre>
   *
   * returns an AsyncIterator of "1", "2", "3"...
   *
   * @param f A function which produces a new CompletionStage from a T
   * @return A new AsyncIterator which produces stages of f composed with the result of the stages
   *     from {@code this} iterator
   */
  public <U> AsyncIterator<U> thenComposeAsync(final Function<T, CompletionStage<U>> f) {
    return supply(
        () -> {
          return safeNextFuture()
              .thenComposeAsync(
                  nt -> {
                    // if there's a value, apply f and wrap the result in an optional,
                    // otherwise an empty result
                    return nt.fold(
                        end -> AsyncIterators.endFuture(),
                        t -> f.apply(t).thenApply(Either::right));
                  });
        });
  }

  /**
   * Creates a new AsyncIterator using the produced stages of {@code f} applied to the output from
   * the stages of {@code this}. {@code f} will be run on the supplied executor.
   *
   * <pre>{@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString, executor);
   * }</pre>
   *
   * returns an AsyncIterator of "1", "2", "3"...
   *
   * @param f A function which produces a new CompletionStage from a T
   * @return A new AsyncIterator which produces stages of f composed with the result of the stages
   *     from {@code this} iterator
   */
  public <U> AsyncIterator<U> thenComposeAsync(
      final Function<T, CompletionStage<U>> f, final Executor executor) {
    return supply(
        () -> {
          return safeNextFuture()
              .thenComposeAsync(
                  nt -> {
                    // if there's a value, apply f and wrap the result in an optional,
                    // otherwise an empty result
                    return nt.fold(
                        end -> AsyncIterators.endFuture(),
                        t -> f.apply(t).thenApply(Either::right));
                  },
                  executor);
        });
  }

  /**
   * Composes f to the stages of {@code this} iterator, and flattens the resulting iterator of
   * iterators.
   *
   * <p>Example:
   *
   * <p>Suppose f is a function that takes an integer i and returns the AsyncIterator of Strings
   * less than i.
   *
   * <pre>
   * intIterator // 1,2,3,...
   *     .thenFlatten(f)
   * </pre>
   *
   * returns an AsyncIterator of "1","1","2","1","2","3"...
   *
   * @param f A function which produces a new AsyncIterator
   * @return A new AsyncIterator consisting of flattened iterators from applying f to elements of
   *     {@code this}
   */
  public <U> AsyncIterator<U> thenFlatten(final Function<T, AsyncIterator<U>> f) {
    return AsyncIterator.concat(this.thenApply(f));
  }

  /**
   * Applies a transformation and flattening to {@code this} iterator with parallelism.
   *
   * <p>This method will consume results from {@code this} sequentially, but will apply the mapping
   * function {@code f} in parallel. The resulting iterator will retain the order of {@code this}.
   * Up to {@code executeAhead} asynchronous operations past what the consumer of the new iterator
   * has already consumed can be started in parallel. <br>
   *
   * @param f A function which produces a new AsyncIterator
   * @param executeAhead An integer indicating the number of allowable calls to f that can be made
   *     ahead of the user has already consumed
   * @return A new AsyncIterator consisting of flattened iterators from apply f to elements of
   *     {@code this}
   * @see #thenFlatten(Function)
   */
  public <U> AsyncIterator<U> thenFlattenAhead(
      final Function<T, AsyncIterator<U>> f, final int executeAhead) {
    final Function<T, CompletionStage<AsyncIterator<U>>> andThen =
        f.andThen(CompletableFuture::completedFuture);
    return AsyncIterator.<U>concat(thenComposeAhead(andThen, executeAhead));
  }

  /**
   * Applies a transformation to {@code this} iterator with parallelism.
   *
   * <p>This method will consume results from {@code this} sequentially, but will apply the mapping
   * function {@code f} in parallel. The resulting iterator will retain the order of {@code this}.
   * Up to {@code executeAhead} asynchronous operations past what the consumer of the new iterator
   * has already consumed can be started in parallel. <br>
   * Like {@link #thenCompose(Function)} but allows for executeAhead
   *
   * @param f A function which produces a new CompletionStage
   * @param executeAhead An integer indicating the number of allowable calls to f that can be made
   *     ahead of the user has already consumed
   * @return A transformed AsyncIterator
   * @see #thenCompose(Function)
   */
  public <U> AsyncIterator<U> thenComposeAhead(
      final Function<T, CompletionStage<U>> f, final int executeAhead) {
    // apply user function and wrap future result in a Option
    final Function<Either<End, T>, CompletionStage<Either<End, U>>> eitherF =
        nt -> {
          return nt.fold(
              stop -> AsyncIterators.endFuture(), t -> f.apply(t).thenApply(Either::right));
        };

    // all queue modifications happen under the async lock
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
          CompletionStage<Either<End, T>> nxt = AsyncIterator.this.safeNextFuture();
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
          final CompletionStage<Either<End, T>> nxt = AsyncIterator.this.safeNextFuture();

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

        StackUnroller.asyncWhile(
            () -> {
              CompletionStage<LockToken> lockFuture = lock.acquireLock();
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
   * Returns a new AsyncIterator which will only produce results that match predicate
   *
   * @param predicate A function that takes a T and returns true if it should be returned by the new
   *     iterator, and false otherwise
   * @return a new AsyncIterator which will only return results that match predicate
   */
  public AsyncIterator<T> filter(final Predicate<T> predicate) {

    // keep looping looking for a value that satisfies predicate as long as the current value
    // doesn't, and we're not out of elements
    final Predicate<Either<End, T>> shouldKeepLooking =
        either -> either.fold(end -> false, predicate.negate()::test);

    return supply(
        () -> {
          return AsyncIterator.this
              .safeNextFuture()
              .thenCompose(
                  t -> {
                    return StackUnroller.<Either<End, T>>asyncWhile(
                        shouldKeepLooking, c -> AsyncIterator.this.safeNextFuture(), t);
                  });
        });
  }

  /**
   * Apply a function and filter an AsyncIterator at the same time
   *
   * @param f a conditional transformation from T to U. If f produces empty, this result will not be
   *     included in the new iterator
   * @return An AsyncIterator of all the Us that were present
   */
  public <U> AsyncIterator<U> filterApply(final Function<T, Optional<U>> f) {
    return this.thenApply(f).filter(Optional::isPresent).thenApply(Optional::get);
  }

  /**
   * Compose and filter an AsyncIterator at the same time
   *
   * @param f an asynchronous conditional transformation from T to U. If f produces empty, this
   *     result will not be included in the new iterator
   * @return An AsyncIterator of all the Us that were present
   */
  public <U> AsyncIterator<U> filterMap(final Function<T, CompletionStage<Optional<U>>> f) {
    return this.thenCompose(f).filter(Optional::isPresent).thenApply(Optional::get);
  }

  /**
   * Returns an AsyncIterator that will return only the first n elements of {@code this}
   * AsyncIterator
   *
   * @param n
   * @return an n element AsyncIterator
   */
  public AsyncIterator<T> take(final long n) {
    return new AsyncIterator<T>() {
      int count = 0;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (++count > n) {
          return AsyncIterators.endFuture();
        } else {
          return AsyncIterator.this.safeNextFuture();
        }
      }
    };
  }

  /**
   * Returns an AsyncIterator that returns elements from the backing iterator until coming across an
   * element that does not satisfy the predicate
   *
   * @param predicate a predicate which returns {@code true} if we can continue returning values
   *     from the iterator, and {@code false otherwise}
   * @return A new AsyncIterator that will return T's until the predicate fails
   */
  public AsyncIterator<T> takeWhile(final Predicate<T> predicate) {
    return new AsyncIterator<T>() {
      boolean predicateFailed = false;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return AsyncIterator.this
            .safeNextFuture()
            .thenApply(
                optionalT -> {
                  return optionalT.flatMap(
                      t -> {
                        if (predicateFailed) {
                          return AsyncIterators.<T>end();
                        } else if (!predicate.test(t)) {
                          predicateFailed = true;
                          return AsyncIterators.<T>end();
                        } else {
                          return Either.<End, T>right(t);
                        }
                      });
                });
      }
    };
  }

  /**
   * Fuses the iterator to the {@link End} result after iteration has stopped.
   *
   * <p>It is normally undefined behavior to call {@link #nextFuture()} after {@link End} has
   * already been returned. On a fused iterator, nextFuture will just continue to return End.
   *
   * @return An iterator where it is safe to call {@link #nextFuture()} after {@link End} has
   *     already been returned
   */
  public AsyncIterator<T> fuse() {
    return new AsyncIterator<T>() {
      boolean end = false;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (end) {
          return AsyncIterators.endFuture();
        }
        return AsyncIterator.this
            .safeNextFuture()
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
  public <A, R> AsyncIterator<R> batch(
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
                .safeNextFuture()
                .thenCompose(
                    optT -> {
                      this.lastAdvance = optT;
                      return collectBatch();
                    })
            : collectBatch();
      }

      private CompletionStage<Either<End, R>> collectBatch() {
        return this.lastAdvance.fold(
            end -> AsyncIterators.endFuture(),
            ignoredT -> {
              final A batch = collector.supplier().get();

              return StackUnroller.asyncWhile(
                      optT -> optT.right().filter(t -> shouldAddToBatch.test(batch, t)).isPresent(),
                      optT -> {
                        collector
                            .accumulator()
                            .accept(batch, optT.right().orElseThrow(IllegalStateException::new));
                        return AsyncIterator.this.safeNextFuture();
                      },
                      this.lastAdvance)
                  .thenApply(
                      optT -> {
                        this.lastAdvance = optT;
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
  public <A, R> AsyncIterator<R> batch(
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
   * Using a function that produces an accumulation from an existing accumulation and a new element,
   * generate an accumulation from applying the function to every element T of {@code this} iterator
   *
   * @param accumulator a function from U,T to U
   * @param identity a starting U value
   * @return a CompletionStage containing the resulting U from repeated application of accumulator
   */
  public <U> CompletionStage<U> fold(final BiFunction<U, T, U> accumulator, final U identity) {
    @SuppressWarnings("unchecked")
    U[] uarr = (U[]) new Object[] {identity};
    return this.collect(
            () -> uarr,
            (u, t) -> {
              uarr[0] = accumulator.apply(uarr[0], t);
            })
        .thenApply(arr -> arr[0]);
  }

  /**
   * The same as {@link #fold(BiFunction, Object)}, but with a function that operates only on Ts
   *
   * @param accumulator a function from T,T to T
   * @param identity a default T value
   * @return a CompletionStage containing the resulting T from repeated application of accumulator
   */
  public CompletionStage<T> fold(final BinaryOperator<T> accumulator, final T identity) {

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
   * Call nextFuture one at a time until reaching the end of the iterator
   *
   * @return A future that is completed when consumption is finished
   */
  public CompletionStage<Void> consume() {
    return FutureSupport.voided(
        safeNextFuture()
            .thenCompose(
                firstValue ->
                    StackUnroller.<Either<End, T>>asyncWhile(
                        Either::isRight, ig -> safeNextFuture(), firstValue)));
  }

  /**
   * Perform a mutable reduction operation using collector and return a CompletionStage of the
   * result.
   *
   * @param collector
   * @return a CompletionStage which will complete with the collected value
   */
  public <R, A> CompletionStage<R> collect(final Collector<? super T, A, R> collector) {
    final A container = collector.supplier().get();
    final BiConsumer<A, ? super T> acc = collector.accumulator();
    return forEach(t -> acc.accept(container, t))
        .thenApply(ig -> AsyncIterators.finishContainer(container, collector));
  }

  /**
   * Perform a mutable reduction operation and return a future of the result.
   *
   * @param supplier
   * @param accumulator
   * @return a CompletionStage which will complete with the accumulated value
   */
  public <R> CompletionStage<R> collect(
      final Supplier<R> supplier, final BiConsumer<R, ? super T> accumulator) {
    final R container = supplier.get();
    return forEach(t -> accumulator.accept(container, t)).thenApply(ig -> container);
  }

  /**
   * Perform the side effecting action until the end of iteration is reached
   *
   * @param action A side-effecting action that takes a T
   * @return A future that returns when action will stop being called
   */
  public CompletionStage<Void> forEach(final Consumer<T> action) {
    return FutureSupport.voided(
        safeNextFuture()
            .thenCompose(
                firstValue ->
                    StackUnroller.<Either<End, T>>asyncWhile(
                        Either::isRight,
                        nt -> {
                          nt.forEach(ig -> {}, action);
                          return safeNextFuture();
                        },
                        firstValue)));
  }

  /**
   * Get the first element that satisfies predicate, or empty if no such element exists
   *
   * @param predicate the predicate that returns true for the desired element
   * @return the first T to satisfy predicate, or empty if no such T exists
   */
  public CompletionStage<Optional<T>> find(final Predicate<T> predicate) {
    return this.filter(predicate).safeNextFuture().thenApply(e -> e.right());
  }

  /**
   * Flatten a list of AsyncIterators into one big AsyncIterator
   *
   * @param asyncIterators an Collection of AsyncIterators to concatenate
   * @return A single AsyncIterator that is the concatenation of asyncIterators
   */
  public static <T> AsyncIterator<T> concat(final Collection<AsyncIterator<T>> asyncIterators) {
    if (asyncIterators.isEmpty()) {
      return AsyncIterator.empty();
    }
    final Queue<AsyncIterator<T>> q = new ArrayDeque<>(asyncIterators);

    return supply(
        () ->
            q.peek()
                .safeNextFuture()
                .thenCompose(
                    first -> {
                      return StackUnroller.<Either<End, T>>asyncWhile(
                          nt -> !nt.isRight() && q.poll() != null && !q.isEmpty(),
                          ig -> q.peek().safeNextFuture(),
                          first);
                    }));
  }

  /**
   * Flatten a list of AsyncIterators into one big AsyncIterator
   *
   * @param asyncIterators
   * @return A single AsyncIterator that is the concatenation of asyncIterators
   */
  public static <T> AsyncIterator<T> concat(final AsyncIterator<AsyncIterator<T>> asyncIterators) {
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
        return StackUnroller.<Either<End, T>>asyncWhile(
            ot -> !ot.isRight() && this.curr != null,
            ot -> {
              return this.curr
                  .safeNextFuture()
                  .thenCompose(
                      optional -> {
                        /*
                         * if the result from the last call to nextFuture() is empty, grab another
                         * AsyncIterator out of asyncIterators and set curr
                         */
                        return optional.fold(
                            end -> {
                              // current iterator was out of elements
                              return asyncIterators
                                  .safeNextFuture()
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
   * Create an iterator that is the result of f applied to iteration elements returned by tIt and
   * uIt
   *
   * <p>If either input stream terminates, the returned iterator will terminate.
   *
   * @param tIt
   * @param uIt
   * @param f
   * @return AsyncIterator of f applied to elements of tIt and uIt
   */
  public static <T, U, V> AsyncIterator<V> zipWith(
      final AsyncIterator<T> tIt, final AsyncIterator<U> uIt, final BiFunction<T, U, V> f) {
    // once all futures are complete, if all are nonempty, then apply f to the args
    return supply(
        () ->
            tIt.safeNextFuture()
                .thenCombine(uIt.safeNextFuture(), (et, eu) -> AsyncIterators.zipWith(et, eu, f)));
  }

  /**
   * Create an empty AsyncIterator
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
   * @param iterator
   * @return A new AsyncIterator which will yield the elements of {@code iterator}
   */
  public static <T> AsyncIterator<T> fromIterator(final Iterator<T> iterator) {
    return supply(
        () ->
            CompletableFuture.completedFuture(
                iterator.hasNext() ? Either.right(iterator.next()) : AsyncIterators.end()));
  }

  /**
   * Creates an AsyncIterator from a CompletionStage of an {@link Iterator}
   *
   * @param future a stage that produces an iterator
   * @return a new AsyncIterator which will yield the elements of the produced iterator
   */
  public static <T> AsyncIterator<T> fromIteratorStage(
      final CompletionStage<AsyncIterator<T>> future) {
    return supply(() -> future.thenCompose(AsyncIterator::safeNextFuture));
  }

  /**
   * Creates an AsyncIterator of one element.
   *
   * @param t the element to return
   * @return an AsyncIterator which yield the element t, and then afterward produce the {@link End}
   *     marker.
   */
  public static <T> AsyncIterator<T> once(final T t) {
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
   * Creates an AsyncIterator for which all downstream operations will be completed with an
   * exception
   *
   * @param ex
   * @return an AsyncIterator that produces exceptional CompletionStages
   */
  public static <T> AsyncIterator<T> error(final Throwable ex) {
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
  public static <T> AsyncIterator<T> repeat(final T t) {
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
  public static AsyncIterator<Integer> range(final int start, final int end, final int delta) {
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
  public static AsyncIterator<Integer> infiniteRange(final int start, final int delta) {
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
   * Create an AsyncIterator from a collection of futures
   *
   * <p>As each future completes, the returned iterator yields a value. As the name implies, the
   * order in which values are returned does not reflect the original order of the collection of
   * futures.
   *
   * @param futures
   * @return AsyncIterator of values produced by futures in order of completion
   */
  public static <T> AsyncIterator<T> unordered(
      final Collection<? extends CompletionStage<T>> futures) {
    final AtomicInteger size = new AtomicInteger(futures.size());
    final AsyncChannel<Either<Throwable, T>> channel = AsyncChannels.unbounded();
    for (CompletionStage<T> future : futures) {
      future.whenComplete(
          (t, ex) -> {
            Either<Throwable, T> toSend = t != null ? Either.right(t) : Either.left(ex);
            channel.send(toSend);
            if (size.decrementAndGet() == 0) {
              // close the channel
              channel.close();
            }
          });
    }
    return channel.thenCompose(
        either -> {
          return either.fold(
              ex -> {
                CompletableFuture<T> future = new CompletableFuture<>();
                future.completeExceptionally(ex);
                return future;
              },
              t -> CompletableFuture.completedFuture(t));
        });
  }

  /**
   * Creates an infinite AsyncIterator of type T.
   *
   * @param supplier supplies stages for elements to be yielded by the returned iterator
   * @return AsyncIterator returning values generated from {@code supplier}
   */
  public static <T> AsyncIterator<T> generate(final Supplier<CompletionStage<T>> supplier) {
    return new AsyncIterator<T>() {
      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return supplier.get().thenApply(Either::right);
      }
    };
  }

  /**
   * Creates an AsyncIterator of type T
   *
   * @param supplier produces CompletionStages of elements in the iterator or indicates end of
   *     iteration with {@link End}
   * @param <T>
   * @return An AsyncIterator that produces the values generated by the {@code supplier}
   */
  public static <T> AsyncIterator<T> supply(
      final Supplier<CompletionStage<Either<End, T>>> supplier) {
    return new AsyncIterator<T>() {
      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return supplier.get();
      }
    };
  }

  /**
   * Successively apply the asynchronous function f to the seed until the future returned by f
   * contains an empty optional or returns an exception. Creates an iterator of values of
   * applications
   *
   * <p>For example, if {@code f = t -> CompletableFuture.completedFuture(Either.right(f(t)))}, then
   * this would produce an asynchronous stream of the values
   *
   * <p>seed, f(seed), f(f(seed)), f(f(f(seed))),...
   *
   * <p>The stream is potentially infinite - it would be in the above example.
   *
   * @param seed the first value returned by the returned iterator
   * @param f the function that is applied to the previous value to generate the next value
   * @return AsyncIterator of the values returned by f
   * @see Stream#iterate(Object, java.util.function.UnaryOperator)
   */
  public static <T> AsyncIterator<T> unfold(
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
