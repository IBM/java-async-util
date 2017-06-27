package com.ibm.async_util;

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

import com.ibm.async_util.AsyncLock.LockToken;

public interface AsyncIterator<T> {

  /**
   * A marker interface that indicates there are no elements left in the iterator
   */
  interface End {
  }

  /**
   * Return a future representing the next element of the iterator. If the optional is empty, the
   * iterator has no more elements. After an iterator returns empty, a consumer should make no
   * subsequent calls to nextFuture
   * 
   * @return A future of the next element for iteration, or empty
   */
  CompletionStage<Either<T, End>> nextFuture();

  /**
   * Like {@link AsyncIterator#flatMap(Function)} but allows for executeAhead
   * 
   * @param f A function which produces a new AsyncIterator
   * @param executeAhead An integer indicating the number of allowable calls to f that can be made
   *        ahead of the user has already consumed
   * @return A new AsyncIterator consisting of flattened iterators from apply f to elements of
   *         {@code this}
   * @see #flatMap(Function)
   */
  default <U> AsyncIterator<U> flatMapAhead(final Function<T, AsyncIterator<U>> f,
      final int executeAhead) {
    final Function<T, CompletionStage<AsyncIterator<U>>> andThen =
        f.andThen(CompletableFuture::completedFuture);
    return AsyncIterator.<U>concat(mapAhead(andThen, executeAhead));
  }

  /**
   * Like {@link #map(Function)} but allows for executeAhead
   * 
   * @param f A function which produces a new ObservableFuture
   * @param executeAhead An integer indicating the number of allowable calls to f that can be made
   *        ahead of the user has already consumed
   * @return A transformed AsyncIterator
   * 
   * @see #map(Function)
   */
  default <U> AsyncIterator<U> mapAhead(final Function<T, CompletionStage<U>> f,
      final int executeAhead) {

    // apply user function and wrap future result in a Option
    final Function<Either<T, End>, CompletionStage<Either<U, End>>> optF = nt -> {
      return nt.fold(t -> {
        try {
          return f.apply(t).thenApply(Either::<U, End>left);
        } catch (Exception e) {
          return AsyncIterators.<Either<U, End>>exceptional(e);
        }
      }, stop -> AsyncIterators.endFuture());
    };

    // all queue modifications happen under the async lock
    return new AsyncIterator<U>() {
      final Queue<CompletionStage<Either<U, End>>> pendingResults = new ArrayDeque<>(executeAhead);
      final FairAsyncLock lock = new FairAsyncLock();

      private CompletionStage<Either<T, End>> fillMore() {
        if (pendingResults.size() >= executeAhead) {
          // don't call nextFuture, we already have enough stuff pending
          return AsyncIterators.endFuture();
        } else {
          // keep filling up the ahead queue
          CompletionStage<Either<T, End>> nxt = AsyncIterator.this.nextFuture();
          pendingResults.add(nxt.thenCompose(optF));
          return nxt;
        }
      }

      @Override
      public CompletionStage<Either<U, End>> nextFuture() {

        final CompletableFuture<Either<U, End>> retFuture = new CompletableFuture<>();

        // whether this simple future already has a buddy in the pendingResults queue
        // just need a local final reference, other things enforce memory barriers
        final boolean[] connected = new boolean[1];
        connected[0] = false;

        StackUnroller.asyncWhile(() -> {
          CompletionStage<LockToken> lockFuture = lock.acquireLock();
          CompletionStage<Either<T, End>> queueFill = lockFuture.thenCompose(token -> {
            if (connected[0]) {
              return fillMore();
            } else {
              // the first time we enter the loop,
              // we will try to associate retFuture with mapped next future
              connected[0] = true;
              final CompletionStage<Either<U, End>> poll = pendingResults.poll();
              if (poll == null) {

                // there was nothing in the queue, associate our returned future with a new
                // nextFuture call
                final CompletionStage<Either<T, End>> nxt = AsyncIterator.this.nextFuture();

                // don't bother adding it to the queue, because we are already listening on it
                AsyncIterators.listen(nxt.thenCompose(optF), retFuture);

                // may continue iteration and add more stuff to the queue that retFuture
                // doesn't care about
                return nxt;
              } else {
                // let our future be tied to the first result that was in the queue
                AsyncIterators.listen(poll, retFuture);
                return fillMore();
              }
            }
          });
          return lockFuture.thenCombine(queueFill, (token, qfill) -> {
            token.releaseLock();
            return qfill.isLeft();
          });
        });
        return retFuture;
      }
    };
  }


  /**
   * Using f, create a new AsyncIterator from applying f to elements of {@code this} iterator, which
   * returns elements from the newly generated iterators.
   * 
   * <p>
   * Example:
   * 
   * <p>
   * Suppose f is a function that takes an integer i and returns the AsyncIterator of Strings less
   * than i.
   * 
   * <pre>
   * intIterator // 1,2,3,...
   *     .flatMap(f)
   * </pre>
   * 
   * returns an AsyncIterator of "1","1","2","1","2","3"...
   * 
   * @param f A function which produces a new AsyncIterator
   * @return A new AsyncIterator consisting of flattened iterators from apply f to elements of
   *         {@code this}
   * @see #flatMapAhead(Function, int)
   * @see Stream#flatMap(Function)
   */
  default <U> AsyncIterator<U> flatMap(final Function<T, AsyncIterator<U>> f) {
    return AsyncIterator.concat(this.convert(f));
  }

  /**
   * Using f, create a new AsyncIterator that will return futures generated by f applied to
   * {@code this} iterator's elements
   * <p>
   * Example:
   * <p>
   * Suppose f is a function that takes an integer i and returns a String of i
   * 
   * <pre>
   * intIterator // 1,2,3,...
   *     .convert(f)
   * </pre>
   * 
   * returns an AsyncIterator of "1","2","3"...
   * 
   * @param f A function which produces a U from the given T
   * @return A transformed AsyncIterator
   * @see Stream#map(Function)
   * 
   */
  default <U> AsyncIterator<U> convert(final Function<? super T, ? extends U> f) {
    return () -> nextFuture().thenApply(ot -> ot.left().map(f));
  }


  /**
   * Using f, asynchronously generate new values from the values of {@code this} iterator to produce
   * a new iterator
   * 
   * When a consumer is using the resulting iterator, a value will be produced from {@code this}
   * iterator, and the future will be produced using f. When the future is ready, the consumer will
   * recieve the U value returned by the f future. If f produces an exception, iteration will end,
   * even if {@code this} iterator has more elements.
   * 
   * <pre>
   * <code>
   * ObservableFuture<String, E> sillyMicroservice(final int i);
   * intIterator // 1, 2, 3
   *   .map(this::sillyMicroservice);
   * </code>
   * </pre>
   * 
   * returns an AsyncIterator of "1", "2", "3"...
   * 
   * 
   * @param f A function which produces a new ObservableFuture from a T
   * @return A transformed AsyncIterator
   * @see #mapAhead(Function, int)
   * @see Stream#map(Function)
   * @see #convert(Function)
   * 
   */
  default <U> AsyncIterator<U> map(final Function<T, CompletionStage<U>> f) {
    return () -> {
      return nextFuture().thenCompose(nt -> {
        // if there's a value, apply f and wrap the result in an optional,
        // otherwise an empty result
        return nt.left().map(f).left().map(future -> future.thenApply(u -> Either.<U, End>left(u)))
            .fold(future -> future, end -> AsyncIterators.endFuture());
      });
    };
  }


  /**
   * Return a new AsyncIterator which will only return results that match predicate
   * 
   * @param predicate A function that takes a T and returns true if it should be returned by the new
   *        iterator, and false otherwise
   * @return a new AsyncIterator which will only return results that match predicate
   * 
   * @see Stream#filter(Predicate)
   */
  default AsyncIterator<T> filter(final Predicate<T> predicate) {

    // keep looping looking for a value that satisfies predicate as long as the current value
    // doesn't, and we're not out of elements
    final Predicate<Either<T, End>> shouldKeepLooking =
        either -> either.left().toOptional().filter(predicate.negate()).isPresent();

    return () -> {
      return AsyncIterator.this.nextFuture().thenCompose(t -> {
        return StackUnroller.<Either<T, End>>asyncWhile(shouldKeepLooking,
            c -> AsyncIterator.this.nextFuture(), t);
      });
    };
  }

  /**
   * Using a function that produces Us from Us and Ts, generate a U from applying the function to
   * every element T of {@code this} iterator
   * 
   * <pre></pre>
   * 
   * @param accumulator a function from U,T to U
   * @param identity a starting U value
   * @return an ObservableFuture containing the resulting U from repeated application of accumulator
   * 
   * @see AsyncIterator#reduce(BinaryOperator, Object)
   * @see Stream#reduce(Object, BiFunction, BinaryOperator)
   */
  default <U> CompletionStage<U> fold(final BiFunction<U, T, U> accumulator, final U identity) {
    @SuppressWarnings("unchecked")
    U[] uarr = (U[]) new Object[] {identity};
    return this.collect(() -> uarr, (u, t) -> {
      uarr[0] = accumulator.apply(uarr[0], t);
    }).thenApply(arr -> arr[0]);
  }


  /**
   * The same as {@link #reduce(BinaryOperator, Object)}, but with a function that operates only on
   * Ts
   * 
   * @param accumulator a function from T,T to T
   * @param identity a default T value
   * @return an ObservableFuture containing the resulting T from repeated application of accumulator
   * 
   * @see #reduce(BiFunction, Object)
   * @see Stream#reduce(Object, BinaryOperator)
   */
  default CompletionStage<T> fold(final BinaryOperator<T> accumulator, final T identity) {

    // don't make this a lambda - otherwise it will look like a BinaryOperator instead of a
    // BiFunction and we'll recurse
    final BiFunction<T, T, T> biAccumulator = new BiFunction<T, T, T>() {
      @Override
      public T apply(final T t, final T u) {
        return accumulator.apply(t, u);
      }
    };
    return fold(biAccumulator, identity);
  }

  /**
   * Return an AsyncIterator that will return only the first n elements of {@code this}
   * AsyncIterator
   * 
   * @param n
   * @return an n element AsyncIterator
   * @see AsyncIterators#takeWhile(AsyncIterator, Predicate)
   */
  default AsyncIterator<T> take(final long n) {
    return new AsyncIterator<T>() {
      int count = 0;

      @Override
      public CompletionStage<Either<T, End>> nextFuture() {
        if (++count > n) {
          return AsyncIterators.endFuture();
        } else {
          return AsyncIterator.this.nextFuture();
        }
      }
    };
  }

  /**
   * Return an AsyncIterator that returns elements from the backing iterator until coming across an
   * element that does not satisfy the predicate
   * 
   * @param iterator
   * @param predicate a predicate which returns {@code true} if we can continue returning values
   *        from the iterator, and {@code false otherwise}
   * @return A new AsyncIterator that will return T's until the predicate fails
   */
  default AsyncIterator<T> takeWhile(final Predicate<T> predicate) {
    return new AsyncIterator<T>() {
      boolean predicateFailed = false;

      @Override
      public CompletionStage<Either<T, End>> nextFuture() {
        return AsyncIterator.this.nextFuture().thenApply(optionalT -> {
          return optionalT.left().flatMap(t -> {
            if (predicateFailed) {
              return AsyncIterators.end();
            } else if (!predicate.test(t)) {
              predicateFailed = true;
              return AsyncIterators.end();
            } else {
              return Either.left(t);
            }
          });
        });
      }
    };
  }


  /**
   * Call nextFuture one at a time until reaching the end of the iterator
   * 
   * @return A future that is completed when consumption is finished
   * @see #consumeWhile(Predicate)
   */
  default CompletionStage<Void> consume() {
    return FutureSupport.voided(nextFuture().thenCompose(firstValue -> StackUnroller
        .<Either<T, End>>asyncWhile(Either::isLeft, ig -> nextFuture(), firstValue)));
  }

  /**
   * Perform a mutable reduction operation using collector and return a future of the result. This
   * is a terminal operation.
   * 
   * @param collector
   * @return
   * 
   * @see Stream#collect(Collector)
   * @see AsyncIterator#collect(Supplier, BiConsumer)
   */
  default <R, A> CompletionStage<R> collect(final Collector<? super T, A, R> collector) {
    final A container = collector.supplier().get();
    final BiConsumer<A, ? super T> acc = collector.accumulator();
    return forEach(t -> acc.accept(container, t))
        .thenApply(ig -> AsyncIterators.finishContainer(container, collector));
  }

  /**
   * Perform a mutable reduction operation and return a future of the result. This is a terminal
   * operation.
   * 
   * @param supplier
   * @param accumulator
   * @return
   * 
   * @see Stream#collect(Supplier, BiConsumer, BiConsumer)
   * @see AsyncIterator#collect(Collector)
   * 
   */
  default <R> CompletionStage<R> collect(final Supplier<R> supplier,
      final BiConsumer<R, ? super T> accumulator) {
    final R container = supplier.get();
    return forEach(t -> accumulator.accept(container, t)).thenApply(ig -> container);
  }

  /**
   * Perform the side effecting action until the end of iteration is reached
   * 
   * @param action A side-effecting action that takes a T
   * @return A future that returns when action will stop being called
   * @see #forEachConditional(Predicate, Consumer)
   */
  default CompletionStage<Void> forEach(final Consumer<T> action) {
    return FutureSupport.voided(nextFuture()
        .thenCompose(firstValue -> StackUnroller.<Either<T, End>>asyncWhile(Either::isLeft, nt -> {
          nt.left().consume(action);
          return nextFuture();
        }, firstValue)));
  }

  /**
   * Get the first element that satisfies predicate, or empty if no such element exists
   * 
   * @param predicate the predicate that returns true for the desired element
   * @return the first T to satisfy predicate, or empty if no such T exists
   */
  default CompletionStage<Optional<T>> find(final Predicate<T> predicate) {
    return this.filter(predicate).nextFuture().thenApply(e -> e.left().toOptional());
  }

  /**
   * Convert and filter an AsyncIterator at the same time
   * 
   * Provide a conditional transformation from T to U.
   * 
   * @param f
   * @return An AsyncIterator of all the Us that were present
   */
  default <U> AsyncIterator<U> filterConvert(final Function<T, Optional<U>> f) {
    return this.convert(f).filter(Optional::isPresent).convert(Optional::get);
  }

  /**
   * Map and filter an AsyncIterator at the same time
   * 
   * Provide a conditional transformation from T to U.
   * 
   * @param f
   * @return An AsyncIterator of all the Us that were present
   */
  default <U> AsyncIterator<U> filterMap(final Function<T, CompletionStage<Optional<U>>> f) {
    return this.map(f).filter(Optional::isPresent).convert(Optional::get);
  }

  /**
   * Collect the results of this iterator in batches, returning an iterator of those batched
   * collections.
   * <p>
   * This may be useful for performing bulk operations on many elements, rather than on one element
   * at a time.
   * 
   * @param collector a {@link Collector} used to collect the elements of this iterator into
   *        individual batches. Each batch will be created by invoking the collector's
   *        {@link Collector#supplier()} method
   * @param shouldAddToBatch a predicate which determines whether a given element encountered during
   *        iteration should be added to the given (current) batch. If this predicate returns true
   *        for the given element and container, the element will be {@link Collector#accumulator()
   *        added} to the container, and the batching operation will continue to draw from the
   *        underlying iterator. If this predicate returns false, the element will not be added and
   *        the current batch will be {@link Collector#finisher() finished} and returned by the
   *        batching iterator. The element which did not meet the predicate will be tested again by
   *        the next batch
   * @return an AsyncIterator which invokes several iterations of the underlying iterator with each
   *         advance, collecting these elements into containers provided by the given
   *         {@link Collector}.
   */
  default <A, R> AsyncIterator<R> batch(final Collector<? super T, A, R> collector,
      final BiPredicate<? super A, ? super T> shouldAddToBatch) {
    return new AsyncIterator<R>() {
      /**
       * This field holds the result of the latest call to the underlying iterator's 'nextFuture';
       * At the start of the batching iterator's 'nextFuture' method, this holds the value which was
       * rejected by the last 'addToBatch' call (or empty if the iterator terminated, or null if
       * this is the first call). If non-empty, this rejected value should be tested again in the
       * next batch. If empty, iteration should terminate
       */
      private Either<T, End> lastAdvance = null;

      @Override
      public CompletionStage<Either<R, End>> nextFuture() {
        // the first call has no preceding value to start the batch, so draw from iter
        return this.lastAdvance == null ? AsyncIterator.this.nextFuture().thenCompose(optT -> {
          this.lastAdvance = optT;
          return collectBatch();
        }) : collectBatch();
      }

      private CompletionStage<Either<R, End>> collectBatch() {
        return this.lastAdvance.fold(ignoredT -> {
          final A batch = collector.supplier().get();

          return StackUnroller.asyncWhile(optT -> optT.left().toOptional()
              .filter(t -> shouldAddToBatch.test(batch, t)).isPresent(), optT -> {
                collector.accumulator().accept(batch,
                    optT.left().toOptional().orElseThrow(IllegalStateException::new));
                return AsyncIterator.this.nextFuture();
              }, this.lastAdvance).thenApply(optT -> {
                this.lastAdvance = optT;
                return Either.left(AsyncIterators.finishContainer(batch, collector));
              });

        }, end -> AsyncIterators.endFuture());
      }
    };
  }

  /**
   * A convenience method provided to invoke {@link #batch(Collector, BiPredicate)} with a predicate
   * that limits batches to a fixed size.
   * <p>
   * Each batch will be as large as the given {@code batchSize} except possibly the last one, which
   * may be smaller due to exhausting the underlying iterator.
   * 
   * @see #batch(Collector, BiPredicate)
   */
  default <A, R> AsyncIterator<R> batch(final Collector<? super T, A, R> collector,
      final int batchSize) {
    class CountingContainer {
      final A container;
      int size;

      public CountingContainer(final A container, final int size) {
        this.container = container;
        this.size = size;
      }
    }

    class CountingCollector implements Collector<T, CountingContainer, R>,
        Supplier<CountingContainer>, BiConsumer<CountingContainer, T>,
        BinaryOperator<CountingContainer>, BiPredicate<CountingContainer, T> {
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
        return countingContainer -> AsyncIterators.finishContainer(countingContainer.container,
            collector);
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

  @SuppressWarnings("unchecked")
  public static <T> AsyncIterator<T> empty() {
    return (AsyncIterator<T>) AsyncIterators.EMPTY_ITERATOR;
  }

  /**
   * Flatten a list of AsyncIterators into one big AsyncIterator
   * 
   * @param asyncIterators
   * @return A single AsyncIterator that is the concatenation of asyncIterators
   */
  public static <T, E extends Exception> AsyncIterator<T> concat(
      final Collection<AsyncIterator<T>> asyncIterators) {
    if (asyncIterators.isEmpty()) {
      return AsyncIterator.empty();
    }
    final Queue<AsyncIterator<T>> q = new ArrayDeque<>(asyncIterators);

    return () -> q.peek().nextFuture().thenCompose(first -> {
      return StackUnroller.<Either<T, End>>asyncWhile(
          nt -> !nt.isLeft() && q.poll() != null && !q.isEmpty(), ig -> q.peek().nextFuture(),
          first);
    });
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
      public CompletionStage<Either<T, End>> nextFuture() {
        if (this.curr == null) {
          // out of iterators
          return AsyncIterators.endFuture();
        }

        /*
         * on each iteration call nextFuture. If it's empty, we should set curr to the next
         * iterator. If we are out of iterators, curr becomes null, and we stop iterating
         */
        return StackUnroller.<Either<T, End>>asyncWhile(ot -> !ot.isLeft() && this.curr != null,
            ot -> {
              return this.curr.nextFuture().thenCompose(optional -> {
                /*
                 * if the result from the last call to nextFuture() is empty, grab another
                 * AsyncIterator out of asyncIterators and set curr
                 */
                return optional.fold(t -> (CompletionStage<Either<T, End>>) CompletableFuture
                    .completedFuture(Either.<T, End>left(t)), end -> {
                      // current iterator was out of elements
                      return asyncIterators.nextFuture().thenApply(nextIt -> {
                        // null if no iterators left
                        this.curr = nextIt.left().orElse(null);
                        // now curr has been updated so a retry will use
                        // the next iterator if it exists
                        return AsyncIterators.end();
                      });
                    });
              });
            },
            // initially pass empty, so we always enter the loop
            AsyncIterators.end());
      }
    };
  }

  public static <T> AsyncIterator<T> fromIterator(final Iterator<T> iterator) {
    return () -> CompletableFuture
        .completedFuture(iterator.hasNext() ? Either.left(iterator.next()) : AsyncIterators.end());
  }

  public static <T> AsyncIterator<T> fromIteratorStage(
      final CompletionStage<AsyncIterator<T>> future) {
    return () -> future.thenCompose(AsyncIterator::nextFuture);
  }

  public static <T> AsyncIterator<T> once(final T t) {
    return new AsyncIterator<T>() {
      Either<T, End> curr = Either.left(t);

      @Override
      public CompletionStage<Either<T, End>> nextFuture() {
        Either<T, End> prev = curr;
        curr = AsyncIterators.end();
        return CompletableFuture.completedFuture(prev);
      }
    };
  }

  public static <T> AsyncIterator<T> repeat(final T t) {
    final Either<T, End> ret = Either.left(t);
    return () -> CompletableFuture.completedFuture(ret);
  }


  /***
   * Create an AsyncIterator for a range.
   * 
   * If delta is positive, similar to for(i = start; start < end; start+=delta). If delta is
   * negative, similar to for(i = start; start > end; start+=delta).
   * 
   * The futures returned by nextFuture will be already completed.
   * 
   * @param start - The start point of iteration (inclusive)
   * @param end - The end point of iteration (exclusive)
   * @param delta
   * @return an AsyncIterator that will return a integers from start to end incremented by delta
   * 
   * @see #infiniteRange(long, long)
   */
  public static AsyncIterator<Integer> range(final int start, final int end, final int delta) {
    if (delta == 0) {
      throw new IllegalArgumentException("increment/decrement must be nonzero");
    }
    return new AsyncIterator<Integer>() {
      int counter = start;

      @Override
      public CompletionStage<Either<Integer, End>> nextFuture() {
        if ((delta > 0 && counter < end) || (delta < 0 && counter > end)) {
          int ret = counter;
          counter += delta;
          return CompletableFuture.completedFuture(Either.left(ret));
        } else {
          return AsyncIterators.endFuture();
        }
      }
    };
  }

  /***
   * Create an Infinite AsyncIterator for a range.
   * 
   * If you try to consume this entire iterator (perhaps by using a 'terminal' operation like
   * {@link AsyncIterator#reduce}), it will infinite loop.
   * 
   * The futures returned by nextFuture will be already completed.
   * 
   * @param start - The start point of iteration (inclusive)
   * @param delta
   * @return an AsyncIterator that will return a integers from start to end incremented by delta
   * 
   * @see #range(long, long, long)
   */
  public static AsyncIterator<Integer> infiniteRange(final int start, final int delta) {
    if (delta == 0) {
      throw new IllegalArgumentException("increment/decrement must be nonzero");
    }
    return new AsyncIterator<Integer>() {
      int counter = start;

      @Override
      public CompletionStage<Either<Integer, End>> nextFuture() {
        int old = counter;
        counter += delta;
        return CompletableFuture.completedFuture(Either.left(old));
      }
    };
  }

  /**
   * Produce an infinite stream of type T.
   * 
   * @param supplier
   * @return AsyncIterator returning values from Supplier
   * 
   * @see Stream#generate(Supplier)
   */
  public static <T, E extends Exception> AsyncIterator<T> generate(
      final Supplier<CompletionStage<T>> supplier) {
    return () -> supplier.get().thenApply(Either::left);
  }

  /**
   * Successively apply the asynchronous function f to the seed until the future returned by f
   * contains an empty optional or returns an exception. Creates an iterator of values of
   * applications
   * 
   * For example, if f = t -> ObservableFutures.success(Optional.of(g(t))), then this would produce
   * an asynchronous stream of the values
   * 
   * seed, f(seed), f(f(seed)), f(f(f(seed))),...
   * 
   * The stream is potentially infinite - it would be in the above example. If you want an infinite
   * stream and plan on never returning Optional.empty, you may use
   * {@link #infiniteIterate(Object, Function)} for convenience.
   * 
   * @param seed
   * @param f
   * @return AsyncIterator of the values returned by f
   * @see Stream#iterate(Object, java.util.function.UnaryOperator)
   */
  public static <T> AsyncIterator<T> unfold(final T seed,
      final Function<T, CompletionStage<Either<T, End>>> f) {
    return new AsyncIterator<T>() {
      CompletionStage<Either<T, End>> prev = CompletableFuture.completedFuture(Either.left(seed));

      @Override
      public CompletionStage<Either<T, End>> nextFuture() {
        // if there was a value, apply f to it
        CompletionStage<Either<T, End>> ret = prev;
        final CompletionStage<Either<T, End>> next =
            this.prev.thenCompose(nxt -> nxt.fold(f, end -> AsyncIterators.endFuture()));
        this.prev = next;
        return ret;
      }
    };
  }


  /**
   * Create an iterator that is the result of f applied to iteration elements returned by tIt and
   * uIt
   * 
   * @param tIt
   * @param uIt
   * @param f
   * @return AsyncIterator of f applied to elements of tIt and uIt {@link #zip}
   */
  public static <T, U, V> AsyncIterator<V> zipWith(final AsyncIterator<T> tIt,
      final AsyncIterator<U> uIt, final BiFunction<T, U, V> f) {
    // once all futures are complete, if all are nonempty, then apply f to the args
    return () -> tIt.nextFuture()
        .thenCompose(nt -> uIt.nextFuture().thenApply(ut -> AsyncIterators.zipWith(nt, ut, f)));
  }

  /**
   * Create an AsyncIterator from a collection of futures
   * 
   * As each future completes, the returned iterator yields a value. As the name implies, the order
   * in which values are returned does not reflect the original order of the collection of futures.
   * 
   * @param futures
   * @return AsyncIterator of values produced by futures in order of completion
   */
  public static <T, E extends Exception> AsyncIterator<T> unordered(
      final Collection<? extends CompletionStage<T>> futures) {
    final AtomicInteger size = new AtomicInteger(futures.size());
    final AsyncChannel<Either<T, Throwable>> channel = AsyncChannels.unbounded();
    for (CompletionStage<T> future : futures) {
      future.whenComplete((t, ex) -> {
        Either<T, Throwable> toSend = t != null ? Either.left(t) : Either.right(ex);
        channel.send(toSend);
        if (size.decrementAndGet() == 0) {
          // close the channel
          channel.close();
        }
      });
    }
    return channel.map(either -> {
      return either.fold(t -> CompletableFuture.completedFuture(t), ex -> {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
      });
    });
  }

  public static <T> AsyncIterator<T> error(final Throwable ex) {
    CompletableFuture<Either<T, End>> future = new CompletableFuture<>();
    future.completeExceptionally(ex);
    return () -> future;
  }
}
