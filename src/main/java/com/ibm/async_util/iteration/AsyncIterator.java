package com.ibm.async_util.iteration;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
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

import com.ibm.async_util.util.AsyncCloseable;
import com.ibm.async_util.util.Either;
import com.ibm.async_util.util.FutureSupport;

/**
 * A mechanism for asynchronously generating and consuming values
 *
 * <p>
 * Consider this an async version of {@link Stream}.
 *
 * <p>
 * AsyncIterators have lazy, pull based, evaluation semantics - values are not computed until they
 * are needed. AsyncIterators are not immutable - like streams, each value they produce is consumed
 * only once. Typically you should not apply multiple transformations to the same source
 * AsyncIterator, it almost certainly won't do what you want it to do.
 *
 * <p>
 * Implementors of the interface need only implement {@link #nextFuture()}. Alternatively, the
 * static methods {@link #generate(Supplier)}, {@link #supply(Supplier)}, and
 * {@link #unfold(Object, Function)} can be used to create new AsyncIterators from functions that
 * generate iteration elements.
 *
 * <p>
 * A note on thread safety: This class makes no assumption that {@link #nextFuture()} is thread
 * safe! Many methods that generate transformed iterators assume that nextFuture will not be called
 * concurrently, and even stronger, that nextFuture won't be called again until the previous future
 * returned by nextFuture has completed.
 *
 * <p>
 * Parallelization may still be accomplished using the <i>partially eager</i> methods described
 * below. The difference is that the parallelization in that case is from <b>producing</b> values in
 * parallel, <b>not consuming</b> values in parallel.
 *
 * <p>
 * To implement an AsyncIterator you must only implement the {@link #nextFuture()} method- however,
 * it is recommended that users avoid actually using nextFuture to consume the results of iteration.
 * It is less expressive and it can also be error prone; it is easy to cause a stack overflow by
 * incorrectly recursing on calls to nextFuture. You should prefer to use the other higher level
 * methods on this interface.
 *
 * <p>
 * There are 2 main categories of such methods on this interface: Intermediate and Terminal. These
 * methods can be combined to form pipelines, which generally consist of a source (often created
 * with the static constructor methods on this interface ({@link #fromIterator(Iterator)},
 * {@link #unfold(Object, Function)}, etc)), followed by zero or more intermediate operations (such
 * as {@link #filter(Predicate)}, {@link #thenApply(Function)}), and completed with a terminal
 * operation which returns a {@link CompletionStage} (such as {@link #forEach(Consumer)} or
 * {@link #fold(BinaryOperator, Object)}). For example, suppose we wanted to accomplish the
 * following (blocking) procedure:
 *
 * <pre>
 * {@code
 * // request and lookup records one by one until we get 10 relevant records
 * List<Record> records = new ArrayList<>()
 * while (records.size() < 10) {
 *     // ask for a record identifier from a remote service (blocking)
 *     RecordId response = requestIdFromIdServer();
 *     // get the actual record from another service (blocking)
 *     Record record = getRecordFromRecordServer(recordIdentifier);
 *     // only add relevant records
 *     if (isRelevant(record)) {
 *        records.add(record);
 *     }
 * }
 * }
 * </pre>
 *
 * If we wanted to do it without doing any blocking, we can use a pipeline and return a
 * {@link CompletionStage} of the desired record list. Like the blocking version only one request
 * will be made at a time.
 *
 * <pre>
 * {@code
 * CompletionStage<RecordId> requestIdFromIdServer();
 * CompletionStage<Record> getRecordFromRecordServer(RecordId recordId);
 *
 * CompletionStage<List<Response>> responses =
 *   AsyncIterator.generate(this::requestIdFromIdServer) // source iterator
 *  .thenCompose(this::getRecordFromRecordServer)        // intermediate transformation
 *  .filter(record -> isRelevant(record))                // intermediate transformation
 *  .take(10)                                            // intermediate transformation
 *  .collect(Collectors.toList());                       // terminal operation
 *
 * }
 * </pre>
 *
 * <p>
 * <b>Intermediate methods</b> - All methods which return {@link AsyncIterator AsyncIterators} are
 * intermediate methods. They can further be broken down into lazy and partially eager methods.
 * Methods that end with the suffix <i>ahead</i> are partially eager, the rest are lazy. A lazy
 * intermediate transformation will not be evaluated until some downstream eager operation is
 * called. Furthermore, only what is needed to satisfy the eager operation will be evaluated from
 * the previous iterator in the chain. When only requesting a single element from the transformed
 * iterator, only a single element may be evaluated from the previous iterator (ex:
 * {@link #thenApply(Function)}), or potentially many elements (ex: {@link #filter(Predicate)}).
 *
 * <p>
 * Methods ending with the suffix <i> ahead </i>, are partially eager. They can be used when there
 * is an expensive transformation step that should be performed in parallel. They will eagerly
 * consume from their upstream iterator up to a specified amount (still sequentially!) and eagerly
 * apply the transformation step.
 *
 * <p>
 * Intermediate methods will propagate exceptions similarly to {@link CompletionStage}, a dependent
 * AsyncIterator will return exceptional stages if the upstream iterator generated exceptional
 * elements.
 *
 * <p>
 * <b>Terminal methods</b> - Terminal methods consume the iterator and return a
 * {@link CompletionStage}. After a terminal operation is called, the iterator is considered
 * consumed and should not be used further. If any of the stages in the chain that comprise
 * {@code this} iterator were exceptional, the {@link CompletionStage} returned by a terminal
 * operation will also be exceptional. The exception will short-circuit the terminal operation. For
 * example, a terminal operation such as {@link #forEach(Consumer)} will not to continue to run on
 * subsequent elements of the iterator and instead immediately complete its returned stage with the
 * error. Unless otherwise noted, this behavior holds for all terminal methods but may not
 * documented explicitly.
 *
 * <p>
 * The exception propagation scheme should be familiar to users of {@link CompletionStage}, upstream
 * errors will appear wherever the AsyncIterator is consumed and the result is observed (with
 * {@link CompletableFuture#join()} for instance). Exceptions at any stage in the pipeline can be
 * recovered from by using {@link #exceptionally(Function)}, however this won't recover exceptions
 * that are produced downstream. A daring user may have applications where they wish to manually
 * iterate past exceptions without converting them. This can be accomplished by using
 * {@link #nextFuture()} directly, see the docs there for more details.
 *
 * <p>
 * Unless otherwise noted, methods on this interface are free to throw {@link NullPointerException}
 * if any of the provided arguments are {@code null}.
 *
 * <p>
 * The behavior of an AsyncIterator if {@link #nextFuture()} is called after the end of iteration
 * marker is returned is left to the implementation. You may ensure that all subsequent calls always
 * return the end marker by using {@link #fuse()}.
 *
 * <p>
 * This interface extends {@link AsyncCloseable}, if there are resources associated with {@code
 * this} iterator that must be relinquished after iteration is complete, the {@link #close()} method
 * should be implemented. Because the majority of methods do not have a manually managed resource, a
 * default implementation of close which does nothing is provided. Terminal methods on this
 * interface do not call {@link #close()}, it is generally the user's responsibility. The exception
 * to this rule are for methods that take AsyncIterators or take functions that produce
 * AsyncIterators, such as {@link #concat(AsyncIterator)} or {@link #thenFlatten(Function)}, in that
 * case intermediate iterators will be automatically closed - any such cases will be documented on
 * the specific method. Similar to the situation with {@link Stream#close()}, because the common
 * case requires no resources the user should only call close if it is possible that the
 * {@link AsyncIterator} has resources.
 *
 * @param <T> Type of object being iterated over.
 * @see Stream
 */
public interface AsyncIterator<T> extends AsyncCloseable {

  /** A marker enum that indicates there are no elements left in the iterator. */
  enum End {
    END;

    private static final Either<End, ?> ITERATION_END = Either.left(End.END);

    private static final CompletionStage<? extends Either<AsyncIterator.End, ?>> END_FUTURE =
        CompletableFuture.completedFuture(ITERATION_END);

    /**
     * An {@link Either} instance which contains the {@link End} enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> Either<AsyncIterator.End, T> end() {
      return (Either<AsyncIterator.End, T>) ITERATION_END;
    }


    /**
     * A {@link CompletionStage} which is already complete, and contains the {@link End#end()}
     * instance as its value.
     */
    @SuppressWarnings("unchecked")
    public static <T> CompletionStage<Either<AsyncIterator.End, T>> endFuture() {
      return (CompletionStage<Either<AsyncIterator.End, T>>) END_FUTURE;
    }

    @Override
    public String toString() {
      return "End of iteration";
    }
  }

  /**
   * Returns a stage that will be completed with the next element of {@code this} iterator when it
   * becomes available, or {@link End} if there are no more elements.
   *
   * <p>
   * This is not a terminal method, it can be safely called multiple times. However, this method is
   * <b>not thread safe</b>, and should only be called in a single-threaded fashion. Moreover,
   * sequential calls should not be made until the {@link CompletionStage} returned by the previous
   * call has completed. That is to say,
   *
   * <pre>
   * {@code
   * // illegal
   * pool.execute(() -> nextFuture())
   * pool.execute(() -> nextFuture())
   *
   * // just as illegal
   * f1 = nextFuture();
   * f2 = nextFuture();
   *
   * // good
   * nextFuture().thenCompose(t -> nextFuture());
   * }
   * </pre>
   *
   * Though this is not a terminal method, if a terminal method has been called it is no longer safe
   * to call this method. When nextFuture returns {@link End}, the iterator has no more elements.
   * After an iterator emits an {@link End} indicator, the result of subsequent calls to nextFuture
   * is undefined.
   *
   * <p>
   * An AsyncIterator may be capable of producing normally completing stages after having producing
   * exceptionally completed stages. nextFuture is unique in that it can safely continue to be
   * called even after a returned stage completes exceptionally, whereas all terminal operations
   * short circuit when encountering an exception. If a user wishes to continue iteration after
   * exception, they must use nextFuture directly, or install exception recovery with
   * {@link #exceptionally(Function)}.
   *
   * @return A {@link CompletionStage} of the next element for iteration held in the
   *         {@link Either#right()} position, or an instance of {@link End} held in the
   *         {@link Either#left()} position indicating the end of iteration.
   */
  CompletionStage<Either<End, T>> nextFuture();

  /**
   * Relinquishes any resources associated with this iterator.
   *
   * <p>
   * This method should be overridden if manual resource management is required, the default
   * implementation does nothing. This method is <b>not</b> thread safe, and must not be called
   * concurrently with calls to {@link #nextFuture()}. This method is not automatically called by
   * terminal methods, and must be explicitly called after iteration is complete if the underlying
   * iterator has resources to release. Similar to the situation with {@link Stream#close()},
   * because the common case requires no resources the user should only call close if it is possible
   * that the {@link AsyncIterator} has resources. Special care needs to be taken to call close even
   * in the case of an exception, {@link AsyncCloseable#tryComposeWith(AsyncCloseable, Function)}
   * can make this more convenient.
   *
   * <pre>
   * {@code
   * class SocketBackedIterator implements AsyncIterator<byte[]> {
   *  ...
   *  {@literal @Override}
   *  CompletionStage<Void> close() { return socket.close(); }
   * }
   * AsyncCloseable.tryComposeWith(new SocketBackedIterator(socket), socketIt -> socketIt
   *  .thenCompose(this::deserialize)
   *  .filter(this::isRelevantMessage)
   *  .forEach(message -> System.out.println(message)));
   * }
   * </pre>
   *
   * Intermediate methods will pass calls to close to their upstream iterators, so it is safe to
   * call close on an intermediate result of an iterator instead of on it directly. For example,
   *
   * <pre>
   * {@code
   * AsyncIterator<byte[]> original = new SocketBackedIterator(socket);
   * AsyncIterator<Message> transformed = original.thenCompose(this::deserialize).filter(this::isRelevantMessage);
   *
   * transformed.close() // will close on original
   * }
   * </pre>
   *
   * @return a {@link CompletionStage} that completes when all resources associated with this
   *         iterator have been relinquished.
   */
  @Override
  default CompletionStage<Void> close() {
    return FutureSupport.voidFuture();
  }

  /**
   * Transforms {@code this} into a new AsyncIterator that iterates over the results of {@code fn}
   * applied to the outcomes of stages in this iterator when they complete normally. When stages in
   * {@code this} iterator complete exceptionally the returned iterator will emit an exceptional
   * stage without applying {@code fn}.
   *
   * <pre>
   * {@code
   * intIterator // 1,2,3,...
   *     .thenApply(Integer::toString) //"1","2","3"...
   * }
   * </pre>
   *
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
   *         from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenApply(final Function<? super T, ? extends U> fn) {
    return AsyncIterators.thenApplyImpl(this, fn, null);
  }

  /**
   * Transforms {@code this} into a new AsyncIterator that iterates over the results of {@code fn}
   * applied to the outcomes of stages in this iterator when they complete normally. When stages in
   * {@code this} iterator complete exceptionally the returned iterator will emit an exceptional
   * stage without applying {@code fn}. {@code fn} is executed with the previous stage's default
   * asynchronous execution facility.
   *
   * <pre>
   * {@code
   * intIterator // 1,2,3,...
   *     .thenApplyAsync(Integer::toString) //"1","2","3"...
   * }
   * </pre>
   *
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a U from the given T
   * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
   *         from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
    return AsyncIterators.thenApplyImpl(this, fn, ForkJoinPool.commonPool());
  }

  /**
   * Transforms {@code this} into a new AsyncIterator that iterates over the results of {@code fn}
   * applied to the outcomes of stages in this iterator when they complete normally. When stages in
   * {@code this} iterator complete exceptionally the returned iterator will emit an exceptional
   * stage without applying {@code fn}. {@code fn} is executed with the provided Executor.
   *
   * <pre>
   * {@code
   * intIterator // 1,2,3,...
   *     .thenApplyAsync(Integer::toString, executor) //"1","2","3"...
   * }
   * </pre>
   *
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a U from the given T
   * @param executor a {@link Executor} where the function {@code fn} should run
   * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
   *         from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenApplyAsync(
      final Function<? super T, ? extends U> fn, final Executor executor) {
    Objects.requireNonNull(executor);
    return AsyncIterators.thenApplyImpl(this, fn, executor);
  }

  /**
   * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
   * applied to the output from the stages of {@code this}. When stages in {@code this} iterator
   * complete exceptionally the returned iterator will emit an exceptional stage without applying
   * {@code fn}.
   *
   * <pre>
   * {@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenCompose(this::asyncToString); //"1", "2", "3"...
   * }
   * </pre>
   *
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a new {@link CompletionStage} from a T
   * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
   *         from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenCompose(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    return AsyncIterators.thenComposeImpl(this, fn, null);
  }

  /**
   * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
   * applied to the output from the stages of {@code this}. When stages in {@code this} iterator
   * complete exceptionally the returned iterator will emit an exceptional stage without applying
   * {@code fn}. {@code fn} will be run on the default asynchronous execution facility of the stages
   * of {@code this}.
   *
   * <pre>
   * {@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenComposeAsync(this::asyncToString); //"1", "2", "3"...
   * }
   * </pre>
   *
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a new {@link CompletionStage} from a T
   * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
   *         from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    return AsyncIterators.thenComposeImpl(this, fn, ForkJoinPool.commonPool());
  }

  /**
   * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
   * applied to the output from the stages of {@code this}. When stages in {@code this} iterator
   * complete exceptionally the returned iterator will emit an exceptional stage without applying
   * {@code fn}. {@code fn} will be run on the supplied executor.
   *
   * <pre>
   * {@code
   * CompletableFuture<String> asyncToString(final int i);
   * intIterator // 1, 2, 3
   *   .thenComposeAsync(this::asyncToString, executor); //"1", "2", "3"...
   * }
   * </pre>
   *
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a new {@link CompletionStage} from a T
   * @param executor a {@link Executor} where the function {@code fn} should run
   * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
   *         from {@code this} iterator
   */
  default <U> AsyncIterator<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn, final Executor executor) {
    Objects.requireNonNull(executor);
    return AsyncIterators.thenComposeImpl(this, fn, executor);
  }

  /**
   * Composes fn with the stages of {@code this} iterator to produce new AsyncIterators, and
   * flattens the resulting iterator of iterators.
   *
   * <p>
   * Suppose we were making requests for locations with an x coordinate and a y coordinate.
   *
   * <pre>
   * {@code
   * CompletableFuture<Z> lookupCoord(int x, int y);
   * AsyncIterator<Z> zs = AsyncIterator.range(0, xmax, 1)
   *  .thenFlatten(x -> AsyncIterator.range(0, ymax, 1)
   *    .thenCompose(y -> lookupCoord(x, y)));
   *
   * // would print z result for (0, 0), (0, 1), (0, 2) ....
   * zs.forEach(z ->  print(z)).toCompletableFuture().join();
   * }
   * </pre>
   *
   * <p>
   * Once all elements from an AsyncIterator produced by {@code fn} have been consumed,
   * {@link #close()} is called on that iterator. If {@link #close()} produces an exception, an
   * exceptional stage will be produced in the returned iterator. If {@link #close()} exceptions
   * should be ignored, they should either be squashed in the iterators produced by {@code fn}, or
   * the consumer may use manual {@link #nextFuture()} iteration to continue past exceptions on the
   * returned iterator. It is still necessary to {@link #close()} the returned iterator, as the last
   * AsyncIterator produced by {@code fn} may have only been partially consumed and would not be
   * closed.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn A function which produces a new AsyncIterator
   * @return A new AsyncIterator consisting of flattened iterators from applying {@code fn} to
   *         elements of {@code this}
   */
  default <U> AsyncIterator<U> thenFlatten(
      final Function<? super T, ? extends AsyncIterator<U>> fn) {
    return AsyncIterator.concat(this.thenApply(fn));
  }

  /**
   * Applies a transformation and flattening to {@code this} iterator with parallelism. This method
   * will consume results from {@code this} sequentially, but will apply the mapping function {@code
   * fn} in parallel. The resulting iterator will retain the order of {@code this}. Up to {@code
   * executeAhead} asynchronous operations past what the consumer of the new iterator has already
   * consumed can be started in parallel.
   *
   * <p>
   * Once all elements from an AsyncIterator produced by {@code fn} have been consumed,
   * {@link #close()} is called on that iterator. If {@link #close()} produces an exception, an
   * exceptional stage will be produced in the handled iterator. If {@link #close()} exceptions
   * should be ignored, they should either be squashed in the input iterators or the consumer may
   * use manual {@link #nextFuture()} iteration to continue past exceptions. It is still necessary
   * to {@link #close()} the returned iterator; this will close {@code this} iterator as well as the
   * up to {@code executeAhead} iterators that have been eagerly produced by {@code fn}.
   *
   * <p>
   * This is a partially eager <i> intermediate </i> method.
   *
   * @param fn A function which produces a new AsyncIterator
   * @param executeAhead An integer indicating the number of allowable calls to {@code fn} that can
   *        be made ahead of the user has already consumed
   * @return A new AsyncIterator consisting of flattened iterators from apply {@code fn} to elements
   *         of {@code this}
   * @see #thenFlatten(Function)
   */
  default <U> AsyncIterator<U> thenFlattenAhead(
      final Function<? super T, ? extends AsyncIterator<U>> fn, final int executeAhead) {
    final Function<Either<End, T>, CompletionStage<Either<End, AsyncIterator<U>>>> eitherF =
        nt -> nt.fold(
            stop -> End.endFuture(),
            t -> CompletableFuture.completedFuture(Either.right(fn.apply(t))));

    final AsyncIterator<AsyncIterator<U>> nestedAsyncIterator =
        new AsyncIterators.PartiallyEagerAsyncIterator<>(
            this,
            executeAhead,
            eitherF,
            // close any unused eagerly evaluated results when we're finished
            AsyncIterator::close);
    return AsyncIterator.concat(nestedAsyncIterator);
  }

  /**
   * Applies a transformation to {@code this} iterator with parallelism. This method will consume
   * results from {@code this} sequentially, but will apply the mapping function {@code fn} in
   * parallel. The resulting iterator will retain the order of {@code this}. Up to {@code
   * executeAhead} asynchronous operations past what the consumer of the new iterator has already
   * consumed can be started in parallel.
   *
   * <p>
   * This is a partially eager <i> intermediate </i> method.
   *
   * @param fn A function which produces a new CompletionStage
   * @param executeAhead An integer indicating the number of allowable calls to fn that can be made
   *        ahead of the user has already consumed
   * @return A transformed AsyncIterator
   * @see #thenCompose(Function)
   */
  default <U> AsyncIterator<U> thenComposeAhead(
      final Function<? super T, ? extends CompletionStage<U>> fn, final int executeAhead) {
    // apply user function and wrap future result in a Either
    final Function<Either<End, T>, CompletionStage<Either<End, U>>> eitherF =
        nt -> nt.fold(stop -> End.endFuture(),
            t -> fn.apply(t).thenApply(Either::right));

    return new AsyncIterators.PartiallyEagerAsyncIterator<>(this, executeAhead, eitherF, null);
  }

  /**
   * Transforms the AsyncIterator into one which will only produce results that match {@code
   * predicate}.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param predicate A function that takes a T and returns true if it should be returned by the new
   *        iterator, and false otherwise
   * @return a new AsyncIterator which will only return results that match predicate
   */
  default AsyncIterator<T> filter(final Predicate<? super T> predicate) {

    // keep looping looking for a value that satisfies predicate as long as the current value
    // doesn't, and we're not out of elements
    final Predicate<Either<End, T>> shouldKeepLooking =
        either -> either.fold(end -> false, predicate.negate()::test);

    return new AsyncIterator<T>() {
      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return AsyncIterator.this
            .nextFuture()
            .thenCompose(
                t -> AsyncTrampoline.asyncWhile(
                    shouldKeepLooking,
                    c -> AsyncIterator.this.nextFuture(),
                    t));
      }

      @Override
      public CompletionStage<Void> close() {
        return AsyncIterator.this.close();
      }
    };
  }

  /**
   * Applies a transformation and filters this AsyncIterator at the same time. Since
   * {@link Optional} cannot hold null values, this method cannot be used to map to an iterator of
   * possibly null types.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn a conditional transformation from {@code T} to {@code U}. If fn produces empty, this
   *        result will not be included in the new iterator
   * @return An AsyncIterator of all the {@code U}s that were present
   */
  default <U> AsyncIterator<U> filterApply(final Function<? super T, Optional<U>> fn) {
    return this.thenApply(fn).filter(Optional::isPresent).thenApply(Optional::get);
  }

  /**
   * Composes and filters an AsyncIterator at the same time. Since {@link Optional} cannot hold null
   * values, this method cannot be used to map to an iterator of possibly null types.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn an asynchronous conditional transformation from T to U. If fn produces empty, this
   *        result will not be included in the new iterator
   * @return An AsyncIterator of all the {@code U}s that were present
   */
  default <U> AsyncIterator<U> filterCompose(
      final Function<? super T, ? extends CompletionStage<Optional<U>>> fn) {
    return this.thenCompose(fn).filter(Optional::isPresent).thenApply(Optional::get);
  }

  /**
   * Returns an AsyncIterator that will return only the first n elements of {@code this}
   * AsyncIterator.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param n the maximum number of elements to take from this iterator
   * @return an AsyncIterator which will return {@code n} elements or less.
   */
  default AsyncIterator<T> take(final long n) {
    return new AsyncIterator<T>() {
      int count = 0;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (++this.count > n) {
          return End.endFuture();
        } else {
          return AsyncIterator.this.nextFuture();
        }
      }

      @Override
      public CompletionStage<Void> close() {
        return AsyncIterator.this.close();
      }
    };
  }

  /**
   * Returns an AsyncIterator that returns elements from the backing iterator until coming across an
   * element that does not satisfy the predicate.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param predicate a predicate which returns {@code true} if we can continue returning values
   *        from the iterator, and {@code false otherwise}
   * @return A new AsyncIterator that will return T's until the predicate fails
   */
  default AsyncIterator<T> takeWhile(final Predicate<? super T> predicate) {
    return new AsyncIterator<T>() {
      boolean predicateFailed = false;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return AsyncIterator.this
            .nextFuture()
            .thenApply(
                either -> either.flatMap(
                    t -> {
                      if (this.predicateFailed) {
                        return End.end();
                      } else if (!predicate.test(t)) {
                        this.predicateFailed = true;
                        return End.end();
                      } else {
                        return Either.right(t);
                      }
                    }));
      }

      @Override
      public CompletionStage<Void> close() {
        return AsyncIterator.this.close();
      }
    };
  }

  /**
   * Returns an AsyncIterator where any exception produced by {@code this} iterator will be
   * transformed with the provided function.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @param fn the Function used to convert an error from this iterator into a T. If {@code fn}
   *        itself throws an exception, that exception will be emitted in the resulting iterator.
   * @return a new AsyncIterator where exceptions from this iterator have been converted using
   *         {@code fn}
   */
  default AsyncIterator<T> exceptionally(final Function<Throwable, ? extends T> fn) {
    return new AsyncIterator<T>() {
      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return AsyncIterators.convertSynchronousException(AsyncIterator.this::nextFuture)
            .exceptionally(ex -> Either.right(fn.apply(ex)));
      }

      @Override
      public CompletionStage<Void> close() {
        return AsyncIterator.this.close();
      }
    };
  }

  /**
   * Fuses the iterator to the {@link End} result after iteration has stopped. It is normally
   * undefined behavior to call {@link #nextFuture()} after {@link End} has already been returned.
   * On a fused iterator, nextFuture will just continue to return End.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
   *
   * @return An iterator where it is safe to call {@link #nextFuture()} after {@link End} has
   *         already been returned
   */
  default AsyncIterator<T> fuse() {
    return new AsyncIterator<T>() {
      boolean end = false;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        if (this.end) {
          return End.endFuture();
        }
        return AsyncIterator.this
            .nextFuture()
            .thenApply(
                either -> {
                  either.forEach(endMarker -> this.end = true, t -> {
                  });
                  return either;
                });
      }

      @Override
      public CompletionStage<Void> close() {
        return AsyncIterator.this.close();
      }
    };
  }

  /**
   * Collect the results of this iterator in batches, returning an iterator of those batched
   * collections.
   *
   * <p>
   * This may be useful for performing bulk operations on many elements, rather than on one element
   * at a time.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
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
  default <A, R> AsyncIterator<R> batch(
      final Collector<? super T, A, R> collector,
      final BiPredicate<? super A, ? super T> shouldAddToBatch) {
    return new AsyncIterator<R>() {
      /**
       * This field holds the result of the latest call to the underlying iterator's 'nextFuture';
       * At the start of the batching iterator's 'nextFuture' method, this holds the value which was
       * rejected by the last 'addToBatch' call (or empty if the iterator terminated, or null if
       * this is the first call). If non-End, this rejected value should be tested again in the next
       * batch. If End, iteration should terminate
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

      @Override
      public CompletionStage<Void> close() {
        return AsyncIterator.this.close();
      }

      private CompletionStage<Either<End, R>> collectBatch() {
        return this.lastAdvance.fold(
            end -> End.endFuture(),
            ignoredT -> {
              final A batch = collector.supplier().get();

              return AsyncTrampoline.asyncWhile(
                  eitherT -> eitherT.fold(end -> false, t -> shouldAddToBatch.test(batch, t)),
                  eitherT -> {
                    collector
                        .accumulator()
                        .accept(batch, eitherT.fold(
                            end -> {
                              throw new IllegalStateException();
                            },
                            t -> t));
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
   * <p>
   * Each batch will be as large as the given {@code batchSize} except possibly the last one, which
   * may be smaller due to exhausting the underlying iterator.
   *
   * <p>
   * This is a lazy <i> intermediate </i> method.
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

      CountingCollector() {
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

  /**
   * Sequentially accumulates the elements of type T in this iterator into a U. This provides an
   * immutable style terminal reduction operation as opposed to the mutable style supported by
   * {@link #collect}. For example, to sum the lengths of Strings in an AsyncIterator, {@code
   * stringIt.fold((acc, s) -> acc + s.length(), 0)}.
   *
   * <p>
   * This is a <i>terminal method</i>.
   *
   * @param accumulator a function that produces a new accumulation from an existing accumulation
   *        and a new element
   * @param identity a starting U value
   * @return a {@link CompletionStage} containing the resulting U from repeated application of
   *         accumulator
   */
  default <U> CompletionStage<U> fold(
      final BiFunction<U, ? super T, U> accumulator, final U identity) {
    @SuppressWarnings("unchecked")
    final U[] uarr = (U[]) new Object[] {identity};
    return this.collect(() -> uarr, (u, t) -> uarr[0] = accumulator.apply(uarr[0], t))
        .thenApply(arr -> arr[0]);
  }

  /**
   * Sequentially accumulates the elements of type T in this iterator into a single T value. This
   * provides an immutable style terminal reduction operation as opposed to the mutable style
   * supported by {@link #collect}. For example, to sum an iterator of ints, {@code intIt.fold((acc,
   * i) -> acc + i, 0)}.
   *
   * <p>
   * This is a <i>terminal method</i>.
   *
   * @param accumulator a function that takes the current accumulated value and a value to fold in
   *        (in that order), and produces a new accumulated value.
   * @param identity a default T value
   * @return a {@link CompletionStage} containing the resulting T from repeated application of
   *         accumulator
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
   * <p>
   * This is a <i>terminal method</i>.
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
   * <p>
   * This is a <i>terminal method</i>.
   *
   * @param collector a {@link Collector} which will sequentially collect the contents of this
   *        iterator into an {@code R}
   * @param <A> The intermediate type of the accumulated object
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
   * <p>
   * This is a <i>terminal method</i>.
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
   * <p>
   * This is a <i>terminal method</i>.
   *
   * @param action a side-effecting action that takes a T
   * @return a {@link CompletionStage} that returns when there are no elements left to apply {@code
   *     action} to, or an exception has been encountered.
   */
  default CompletionStage<Void> forEach(final Consumer<? super T> action) {
    return AsyncTrampoline.asyncWhile(
        () -> nextFuture()
            .thenApply(
                eitherT -> {
                  eitherT.forEach(
                      ig -> {
                      },
                      action);
                  return eitherT.isRight();
                }));
  }

  /**
   * Gets the first element that satisfies predicate, or empty if no such element exists
   *
   * <p>
   * This is a <i>terminal method</i>.
   *
   * @param predicate the predicate that returns true for the desired element
   * @return a {@link CompletionStage} that completes with the first T to satisfy predicate, or
   *         empty if no such T exists
   */
  default CompletionStage<Optional<T>> find(final Predicate<? super T> predicate) {
    final CompletionStage<Either<End, T>> future = AsyncIterators
        .convertSynchronousException(this.filter(predicate)::nextFuture);
    return future.thenApply((final Either<End, T> e) -> e.right());
  }

  /**
   * Flatten a collection of AsyncIterators into a single AsyncIterator. If the collection has a
   * defined order, that order is preserved in the concatenated result.
   *
   * <pre>
   * {@code
   * // returns an AsyncInterator of 0,1,2,3,4
   * AsyncIterators.concat(Arrays.asList(
   *   AsyncIterators.range(0, 3, 1),
   *   AsyncIterators.range(3, 5, 1)))
   * }
   * </pre>
   *
   * Once all elements from an input AsyncIterator have been consumed, {@link #close()} is called on
   * that iterator. If {@link #close()} produces an exception, an exceptional stage will be produced
   * in the handled iterator. If {@link #close()} exceptions should be ignored, they should either
   * be squashed in the input iterators or the consumer may use manual {@link #nextFuture()}
   * iteration to continue past exceptions. It is still necessary to {@link #close()} the returned
   * iterator, as the last used AsyncIterator may only be partially consumed.
   *
   * @param asyncIterators an Iterator of AsyncIterators to concatenate
   * @return A single AsyncIterator that is the concatenation of asyncIterators
   */
  static <T> AsyncIterator<T> concat(final Iterator<? extends AsyncIterator<T>> asyncIterators) {
    if (!asyncIterators.hasNext()) {
      return AsyncIterator.empty();
    }

    class ConcatAsyncIterator implements AsyncIterator<T> {
      private AsyncIterator<T> current = asyncIterators.next();

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return AsyncIterators.asyncWhileAsyncInitial(
            et -> !et.isRight() && asyncIterators.hasNext(),
            /*
             * when reaching the end of one iterator, it must be closed before opening a new one. if
             * the `close` future yields an error, an errorOnce iterator is concatenated with that
             * close's exception, so the poll on the ConcatIter would encounter this exception. By
             * using an errorOnce iter, the caller could choose to ignore the exception and attempt
             * iterating again, which will pop the next asyncIterator off the meta iterator
             */
            ot -> FutureSupport.thenComposeOrRecover(
                AsyncIterators.convertSynchronousException(this.current::close),
                (t, throwable) -> {
                  this.current =
                      throwable == null
                          ? asyncIterators.next()
                          : AsyncIterators.errorOnce(throwable);
                  return this.current.nextFuture();
                }),
            this.current.nextFuture());
      }

      @Override
      public CompletionStage<Void> close() {
        return this.current.close();
      }

      @Override
      public String toString() {
        return "ConcatCloseableAsyncIter [current="
            + this.current
            + ", iter="
            + asyncIterators
            + "]";
      }
    }

    return new ConcatAsyncIterator();
  }

  /**
   * Flatten an AsyncIterator of AsyncIterators into a single AsyncIterator
   *
   * <pre>
   * {@code
   * // returns an AsyncInterator of 0,1,2,0,1,2,0,1,2
   * AsyncIterators.concat(AsyncIterators.generate(() -> AsyncIterators.range(0, 3, 1)).take(3))
   * }
   * </pre>
   *
   * Once all elements from an input AsyncIterator have been consumed, {@link #close()} is called on
   * that iterator. If {@link #close()} produces an exception, an exceptional stage will be produced
   * in the handled iterator. If {@link #close()} exceptions should be ignored, they should either
   * be squashed in the input iterators or the consumer may use manual {@link #nextFuture()}
   * iteration to continue past exceptions. It is still necessary to {@link #close()} the returned
   * iterator; this will close both {@code asyncIterators} as well as the last used AsyncIterator if
   * it was only partially consumed.
   *
   * @param asyncIterators a AsyncIterator of AsyncIterators
   * @return A single AsyncIterator that is the concatenation of {@code asyncIterators}
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
          return End.endFuture();
        }

        /*
         * on each iteration call nextFuture. If it's empty, we should set curr to the next
         * iterator. If we are out of iterators, curr becomes null, and we stop iterating
         */
        return AsyncIterators.asyncWhileAsyncInitial(
            either -> !either.isRight() && this.curr != null,
            either -> {
              /*
               * if the result from the last call to nextFuture() is empty, grab another
               * AsyncIterator out of asyncIterators and set curr
               */
              return either.fold(
                  end -> {
                    // current iterator was out of elements
                    return FutureSupport.thenComposeOrRecover(
                        /*
                         * when reaching the end of one iterator, it must be closed before opening a
                         * new one. if the `close` future yields an error, an errorOnce iterator is
                         * concatenated with that close's exception, so the poll on the ConcatIter
                         * would encounter this exception. By using an errorOnce iter, the caller
                         * could choose to ignore the exception and attempt iterating again, which
                         * will pop the next asyncIterator off the meta iterator
                         */
                        AsyncIterators.convertSynchronousException(this.curr::close),
                        (t, throwable) -> throwable == null
                            ? asyncIterators.nextFuture()
                            : CompletableFuture.completedFuture(
                                Either.right(AsyncIterators.errorOnce(throwable))))
                        .thenCompose(nextIt -> {
                          // null if no iterators left
                          this.curr = nextIt.right().orElse(null);

                          // return the next future from the newly updated curr
                          return this.curr != null
                              ? this.curr.nextFuture()
                              : End.endFuture();
                        });
                  },
                  t -> CompletableFuture.completedFuture(either));
            },
            this.curr.nextFuture());
      }

      @Override
      public CompletionStage<Void> close() {
        if (this.curr == null) {
          return asyncIterators.close();
        }
        return FutureSupport.thenComposeOrRecover(
            AsyncIterators.convertSynchronousException(asyncIterators::close),
            (t, ex) -> {
              CompletionStage<Void> ret = this.curr.close();
              if (ex != null) {
                ret = ret.thenCompose(ig -> FutureSupport.errorStage(ex));
              }
              return ret;
            });
      }
    };
  }

  /**
   * Create an iterator that is the result of fn applied to iteration elements returned by tIt and
   * uI. If either input iterator terminates, the returned iterator will terminate. If either input
   * iterator returns an exception, an exceptional result will be emitted by the returned iterator.
   * In the case of an exception, a single result will still be consumed from both iterators.
   *
   * <p>
   * When the returned iterator is {@link #close() closed}, the stage returned by close will be
   * complete when both {@code tIt} and {@code uIt} have been closed.
   *
   * @param tIt an AsyncIterator of Ts
   * @param uIt an AsyncIterator of Us
   * @param fn a function that produces a V from a T and a U
   * @return AsyncIterator of fn applied to elements of tIt and uIt
   */
  static <T, U, V> AsyncIterator<V> zipWith(
      final AsyncIterator<T> tIt,
      final AsyncIterator<U> uIt,
      final BiFunction<? super T, ? super U, V> fn) {
    // once all futures are complete, if all are nonempty, then apply fn to the arg
    return new AsyncIterator<V>() {
      @Override
      public CompletionStage<Either<End, V>> nextFuture() {
        // call nextFuture before checking for an exception
        final CompletionStage<Either<End, T>> tFuture =
            AsyncIterators.convertSynchronousException(tIt::nextFuture);
        final CompletionStage<Either<End, U>> uFuture =
            AsyncIterators.convertSynchronousException(uIt::nextFuture);
        return tFuture.thenCombine(uFuture, (et, eu) -> AsyncIterators.zipWith(et, eu, fn));
      }

      @Override
      public CompletionStage<Void> close() {
        return CompletableFuture.allOf(
            AsyncIterators.convertSynchronousException(tIt::close).toCompletableFuture(),
            AsyncIterators.convertSynchronousException(uIt::close).toCompletableFuture());
      }
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
  static <T> AsyncIterator<T> fromIterator(final Iterator<? extends T> iterator) {
    return () -> CompletableFuture.completedFuture(
        iterator.hasNext() ? Either.right(iterator.next()) : End.end());
  }

  /**
   * Creates an AsyncIterator from a {@link CompletionStage} of an {@link Iterator}.
   *
   * @param stage a {@link CompletionStage} stage that produces an AsyncIterator
   * @return a new AsyncIterator which will yield the elements of the produced iterator
   */
  static <T> AsyncIterator<T> fromIteratorStage(
      final CompletionStage<? extends AsyncIterator<T>> stage) {
    return new AsyncIterator<T>() {
      AsyncIterator<T> iter;

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        return this.iter == null
            ? stage.thenCompose(it -> {
              this.iter = it;
              return it.nextFuture();
            })
            : this.iter.nextFuture();
      }

      @Override
      public CompletionStage<Void> close() {
        return this.iter == null
            ? stage.thenCompose(it -> {
              this.iter = it;
              return it.close();
            })
            : this.iter.close();
      }
    };
  }

  /**
   * Creates an AsyncIterator of one element.
   *
   * @param t the element to return
   * @return an AsyncIterator which yields the element t, and then afterward produce the {@link End}
   *         marker.
   */
  static <T> AsyncIterator<T> once(final T t) {
    return new AsyncIterator<T>() {
      Either<End, T> curr = Either.right(t);

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        final Either<End, T> prev = this.curr;
        this.curr = End.end();
        return CompletableFuture.completedFuture(prev);
      }
    };
  }

  /**
   * Creates an AsyncIterator for which all downstream operations will be completed with an
   * exception.
   *
   * @param ex the exception which the {@link CompletionStage CompletionStages} of the returned
   *        future will be completed with
   * @return an AsyncIterator that produces exceptional CompletionStages
   */
  static <T> AsyncIterator<T> error(final Throwable ex) {
    final CompletionStage<Either<End, T>> stage = FutureSupport.errorStage(ex);
    return () -> stage;
  }

  /**
   * Creates an infinite AsyncIterator of the same value.
   *
   * @param t the value to repeat
   * @return An AsyncIterator that will always return {@code t}
   */
  static <T> AsyncIterator<T> repeat(final T t) {
    final CompletableFuture<Either<End, T>> ret =
        CompletableFuture.completedFuture(Either.right(t));
    return () -> ret;
  }

  /**
   * Create an AsyncIterator for a range.
   *
   * <p>
   * If delta is positive, similar to {@code for(i = start; i < end; i+=delta)}. If delta is
   * negative, similar to {@code for(i = start; i > end; i+=delta)}.
   *
   * <p>
   * The futures returned by nextFuture will be already completed.
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
        if ((delta > 0 && this.counter < end) || (delta < 0 && this.counter > end)) {
          final int ret = this.counter;
          this.counter += delta;
          return CompletableFuture.completedFuture(Either.right(ret));
        } else {
          return End.endFuture();
        }
      }
    };
  }

  /**
   * Create an infinite AsyncIterator for a range.
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
        final int old = this.counter;
        this.counter += delta;
        return CompletableFuture.completedFuture(Either.right(old));
      }
    };
  }

  /**
   * Create an AsyncIterator from a collection of {@link CompletionStage CompletionStages}. When a
   * stage completes, the value becomes available for consumption in the returned iterator. If a
   * stage completes exceptionally, the returned iterator will emit an exceptional stage. The order
   * in which values are returned does not reflect the original order of the collection of stages.
   *
   * @param stages a Collection of {@link CompletionStage CompletionStages} that will be emitted in
   *        the returned iterator as they complete
   * @return AsyncIterator of values produced by stages in order of completion
   */
  static <T> AsyncIterator<T> unordered(final Collection<? extends CompletionStage<T>> stages) {
    final int size = stages.size();
    if (size == 0) {
      return AsyncIterator.empty();
    }
    final AtomicInteger count = new AtomicInteger();
    final AsyncChannel<Either<Throwable, T>> channel = AsyncChannels.unbounded();
    for (final CompletionStage<T> future : stages) {
      future.whenComplete((t, ex) -> {
        final Either<Throwable, T> toSend = ex != null ? Either.left(ex) : Either.right(t);
        channel.send(toSend);
        if (count.incrementAndGet() == size) {
          // terminate the channel
          channel.terminate();
        }
      });
    }
    return channel.thenCompose(either -> either.fold(
        FutureSupport::errorStage,
        CompletableFuture::completedFuture));
  }

  /**
   * Creates an infinite AsyncIterator of type T.
   *
   * @param supplier supplies stages for elements to be yielded by the returned iterator
   * @return AsyncIterator returning values generated from {@code supplier}
   */
  static <T> AsyncIterator<T> generate(final Supplier<? extends CompletionStage<T>> supplier) {
    return () -> supplier.get().thenApply(Either::right);
  }

  /**
   * Creates an AsyncIterator of type T
   *
   * @param supplier produces CompletionStages of elements in the iterator or indicates end of
   *        iteration with {@link End}
   * @return An AsyncIterator that produces the values generated by the {@code supplier}
   */
  static <T> AsyncIterator<T> supply(
      final Supplier<? extends CompletionStage<Either<End, T>>> supplier) {
    return supplier::get;
  }

  /**
   * Successively apply the asynchronous function f to the seed until the future returned by f
   * contains an empty optional or returns an exception. Creates an iterator of values of
   * applications
   *
   * <p>
   * For example, if {@code f = t -> CompletableFuture.completedFuture(Either.right(f(t)))}, then
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
      final T seed, final Function<? super T, ? extends CompletionStage<Either<End, T>>> f) {
    return new AsyncIterator<T>() {
      CompletionStage<Either<End, T>> prev = CompletableFuture.completedFuture(Either.right(seed));

      @Override
      public CompletionStage<Either<End, T>> nextFuture() {
        // if there was a value, apply f to it
        final CompletionStage<Either<End, T>> ret = this.prev;
        this.prev = this.prev.thenCompose(
            nxt -> nxt.fold(end -> End.endFuture(), f));
        return ret;
      }
    };
  }
}
