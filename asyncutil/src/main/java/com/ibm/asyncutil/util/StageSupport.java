/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Utility methods for creating and composing {@link CompletionStage CompletionStages}
 * 
 * @author Ravi Khadiwala
 * @author Renar Narubin
 */
public class StageSupport {
  private StageSupport() {}

  /**
   * Gets an already completed {@link CompletionStage} of Void. This common static instance can be
   * used as an alternative to {@code StageSupport.<Void>completedStage(null)}
   *
   * <p>
   * This has a few advantages:
   *
   * <ul>
   * <li>Depending on context, StageSupport.completedStage(null) could either mean a {@code
   * CompletionStage<Void>} or an {@code CompletionStage<T>}. Using this method clearly indicates
   * that we are returning a void future, not a T future with a null result.
   * <li>Immediately completed null futures are very common. Since they are final and static, we can
   * just reuse a single object and save allocations
   * </ul>
   *
   * @return An immediately completed {@link CompletionStage} of {@code Void}
   */
  public static CompletionStage<Void> voidStage() {
    return CompletedStage.VOID;
  }

  /**
   * Creates a {@link CompletionStage} that completes when {@code stage} completes but ignores the
   * result.
   *
   * @param stage a non-void {@link CompletionStage}
   * @return a {@link CompletionStage} of type Void which completes when {@code stage} completes
   */
  public static <T> CompletionStage<Void> voided(final CompletionStage<T> stage) {
    return stage.thenApply(ig -> null);
  }

  /**
   * Creates a {@link CompletionStage} that is already completed with the given value.
   * <p>
   * Non-Async methods on the returned stage will run their dependent actions immediately on the
   * calling thread.
   * <p>
   * Async methods which do not supply an executor (and don't involve another stage) will use the
   * same default executor as used by {@link CompletableFuture}. If another stage is involved (e.g.
   * {@link CompletionStage#thenAcceptBothAsync(CompletionStage, java.util.function.BiConsumer)
   * thenAcceptBothAsync}) then the other stage's default execution facility is used.
   * <p>
   * A completed exceptional stage can be similarly created with the method
   * {@link #exceptionalStage(Throwable)}
   * 
   * @param t the value to be held by the returned stage
   * @return a {@link CompletionStage} that has already been completed with {@code t}
   * @see #exceptionalStage(Throwable)
   */
  public static <T> CompletionStage<T> completedStage(final T t) {
    return CompletedStage.of(t);
  }

  /**
   * Creates a {@link CompletionStage} that is already completed exceptionally. This is the
   * exceptional analog of {@link #completedStage(Object)}.
   * <p>
   * Non-Async methods on the returned stage will run their dependent actions immediately on the
   * calling thread.
   * <p>
   * Async methods which do not supply an executor (and don't involve another stage) will use the
   * same default executor as used by {@link CompletableFuture}. If another stage is involved (e.g.
   * {@link CompletionStage#thenAcceptBothAsync(CompletionStage, java.util.function.BiConsumer)
   * thenAcceptBothAsync}) then the other stage's default execution facility is used.
   *
   * @param ex the exception that completes the returned stage
   * @return a {@link CompletionStage} that has already been completed exceptionally with {@code ex}
   * @see #completedStage(Object)
   */
  public static <T> CompletionStage<T> exceptionalStage(final Throwable ex) {
    return CompletedStage.exception(ex);
  }

  /**
   * Performs a function with an asynchronously acquired {@link AutoCloseable auto closeable},
   * ensuring that the resource is {@link AutoCloseable#close() closed} after the function runs.
   * Similar to a try-with-resources block, the resource will be closed even if {@code fn} throws an
   * exception.
   *
   * <p>
   * The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   * <li>{@code resource} completes exceptionally
   * <li>{@code fn} throws an exception
   * <li>{@link AutoCloseable#close()} throws an exception
   * </ol>
   *
   * Of these cases, only 2 and 3 can happen simultaneously - in this case, the exception thrown by
   * close will be added to the exception from {@code fn} as a suppressed exception. If
   * {@link AutoCloseable#close()} throws a non-runtime exception, it will be wrapped in a
   * {@link CompletionException}.
   *
   * @param resource a {@link CompletionStage} that completes with an {@link AutoCloseable}
   * @param fn an function to perform that uses result of {@code resource} to produce a value
   * @param <T> the result type of {@code fn}
   * @param <R> the {@link AutoCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code fn} or completes
   *         exceptionally
   */
  public static <T, R extends AutoCloseable> CompletionStage<T> tryWith(
      final CompletionStage<R> resource,
      final Function<? super R, ? extends T> fn) {
    return resource.thenApply(r -> {
      try {
        try (R rtemp = r) {
          return fn.apply(r);
        }
      } catch (final RuntimeException rte) {
        throw rte;
      } catch (final Throwable ex) {
        throw new CompletionException(ex);
      }
    });
  }

  /**
   * Performs an asynchronous function with an asynchronously acquired {@link AutoCloseable},
   * ensuring that the resource is {@link AutoCloseable#close() closed} after the stage returned by
   * the function completes. Similar to a try-with-resources block, the resource will be closed even
   * if {@code fn} throws an exception or if the returned stage completes exceptionally.
   *
   * <p>
   * The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   * <li>{@code resource} completes exceptionally
   * <li>{@code fn} throws an exception
   * <li>{@code fn} returns an stage that completes exceptionally
   * <li>{@link AutoCloseable#close()} throws an exception
   * </ol>
   *
   * Of these cases, (2 and 4) and (3 and 4) can happen simultaneously - in these cases, the
   * exception thrown by close will be added to the exception from {@code fn} as a suppressed
   * exception. If {@link AutoCloseable#close()} throws a non-runtime exception, it will be wrapped
   * in a {@link CompletionException}.
   *
   * @param resource a {@link CompletionStage} that completes with an {@link AutoCloseable} which
   *        will be {{@link AutoCloseable#close()} closed} when the stage returned by {@code fn}
   *        completes.
   * @param fn an function to perform that uses result of {@code resource} to produce a new
   *        {@link CompletionStage}
   * @param <T> the type of the {@link CompletionStage} produced by {@code fn}
   * @param <R> the {@link AutoCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code fn} or completes
   *         exceptionally
   */
  public static <T, R extends AutoCloseable> CompletionStage<T> tryComposeWith(
      final CompletionStage<R> resource,
      final Function<? super R, ? extends CompletionStage<T>> fn) {
    return resource.thenCompose(r -> {
      final CompletableFuture<T> ret = new CompletableFuture<>();
      try {
        fn.apply(r).whenComplete((t, actionEx) -> {
          try {
            r.close();
            if (actionEx != null) {
              ret.completeExceptionally(actionEx);
            } else {
              ret.complete(t);
            }
          } catch (final Exception closeEx) {
            if (actionEx != null) {
              actionEx.addSuppressed(closeEx);
              ret.completeExceptionally(actionEx);
            } else {
              ret.completeExceptionally(closeEx);
            }
          }
        });
      } catch (final Throwable actionException) {
        try {
          r.close();
        } catch (final Exception closeException) {
          actionException.addSuppressed(closeException);
        }
        ret.completeExceptionally(actionException);
      }
      return ret;
    });
  }

  /**
   * Performs a function with an {@link AsyncCloseable} resource, ensuring that the resource is
   * {@link AsyncCloseable#close()} closed} after the function completes. The returned stage will
   * complete when the close stage has completed. Similar to a try-with-resources block, the
   * resource will be closed even if {@code fn} throws an exception.
   *
   * <p>
   * The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   * <li>{@code fn} throws an exception
   * <li>{@link AutoCloseable#close()} throws an exception
   * <li>{@link AutoCloseable#close()} returns a stage that completes exceptionally
   * </ol>
   *
   * If the function produces an exception <b> and </b> {@link AsyncCloseable#close()} produces an
   * exception the exception produced by close will be added to the exception from {@code fn} as a
   * {@link Throwable#addSuppressed(Throwable) suppressed exception}.
   *
   * @param resource an {@link AsyncCloseable} which will be {{@link AsyncCloseable#close()} closed}
   *        after {@code fn} is run
   * @param fn a function to perform that uses {@code resource} to produce a result
   * @param <T> the type produced by {@code fn}
   * @param <R> the {@link AsyncCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code fn} after close has
   *         completed or completes exceptionally
   */
  public static <T, R extends AsyncCloseable> CompletionStage<T> tryWith(
      final R resource, final Function<? super R, ? extends T> fn) {

    try {
      final T t = fn.apply(resource);
      try {
        return resource.close().thenApply(ig -> t);
      } catch (final Throwable ex) {
        return exceptionalStage(ex);
      }
    } catch (final Throwable ex) {
      try {
        return resource
            .close()
            .handle((ig, closeEx) -> {
              if (closeEx != null) {
                ex.addSuppressed(closeEx);
              }
              throw ex;
            });
      } catch (final Throwable closeEx) {
        ex.addSuppressed(closeEx);
        return exceptionalStage(ex);
      }
    }
  }

  /**
   * Performs an asynchronous function with an {@link AsyncCloseable} resource, ensuring that the
   * resource is {@link AsyncCloseable#close()} closed} after the stage returned by the function
   * completes. The returned stage will complete when the close stage has completed. Similar to a
   * try-with-resources block, the resource will be closed even if {@code fn} throws an exception or
   * if its returned stage completes exceptionally.
   *
   * <p>
   * The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   * <li>{@code fn} throws an exception
   * <li>{@code fn} returns a stage that completes exceptionally
   * <li>{@link AutoCloseable#close()} throws an exception
   * <li>{@link AutoCloseable#close()} returns a stage that completes exceptionally
   * </ol>
   *
   * If the function produces an exception (cases 1 or 2) <b> and </b>
   * {@link AsyncCloseable#close()} produces an exception (cases 3 or 4) the exception produced by
   * close will be added to the exception from {@code fn} as a
   * {@link Throwable#addSuppressed(Throwable) suppressed exception}.
   *
   * @param resource an {@link AsyncCloseable} which will be {{@link AsyncCloseable#close()} closed}
   *        when the stage returned by {@code fn} completes.
   * @param fn a function to perform that uses {@code resource} to produce a new
   *        {@link CompletionStage}
   * @param <T> the type of the {@link CompletionStage} produced by {@code fn}
   * @param <R> the {@link AsyncCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code fn} after close has
   *         completed or completes exceptionally
   */
  public static <T, R extends AsyncCloseable> CompletionStage<T> tryComposeWith(
      final R resource,
      final Function<? super R, ? extends CompletionStage<T>> fn) {
    try {
      final CompletionStage<T> stage = fn.apply(resource);
      final CompletableFuture<T> ret = new CompletableFuture<>();
      stage.whenComplete((t, ex) -> {
        try {
          resource.close().whenComplete((ig, closeEx) -> {
            if (ex != null) {
              if (closeEx != null) {
                ex.addSuppressed(closeEx);
              }
              ret.completeExceptionally(ex);
            } else if (closeEx != null) {
              ret.completeExceptionally(closeEx);
            } else {
              ret.complete(t);
            }
          });
        } catch (final Throwable closeEx) {
          if (ex != null) {
            ex.addSuppressed(closeEx);
            ret.completeExceptionally(ex);
          } else {
            ret.completeExceptionally(closeEx);
          }
        }
      });
      return ret;
    } catch (final Throwable ex) {
      final CompletableFuture<T> ret = new CompletableFuture<>();
      try {
        resource.close().whenComplete((ig, closeEx) -> {
          if (closeEx != null) {
            ex.addSuppressed(closeEx);
          }
          ret.completeExceptionally(ex);
        });
      } catch (final Throwable closeEx) {
        ex.addSuppressed(closeEx);
        ret.completeExceptionally(ex);
      }
      return ret;
    }
  }

  /**
   * Uses the possibly exceptional result of {@code stage} to produce a new stage.
   *
   * <p>
   * When {@code stage} completes, {@code fn} will be applied with the result of the stage (or null
   * if it completed exceptionally) and the exception from the stage (or null if it completed
   * normally) to produce a new stage. The returned stage will be completed with the outcome of the
   * stage produced by {@code fn}.
   *
   * <pre>
   * {@code
   * CompletionStage<Integer> backupValue(){...};
   * int compute(int i) {
   *    if (i % 2 == 0)
   *        return i / 2;
   *    else
   *        throw new OddException();
   * }
   * // get a stage that has the computed value if compute completes normally,
   * // or asynchronously computes the backup value otherwise.
   * CompletionStage<Optional<Integer>> optionalStage = thenComposeOrRecover(
   *    () -> CompletableFuture.supplyAsync(compute(myInt)),
   *    (result, throwable) -> throwable == null ? backupValue() : CompletableFuture.completedOf(result));
   * }
   * </pre>
   *
   * @param stage a {@link CompletionStage} that may complete exceptionally
   * @param fn a function that will run with the outcome of {@code stage} to produce a new
   *        {@link CompletionStage}
   * @param <T> the type {@code stage}
   * @param <U> the type of the returned {@link CompletionStage}
   * @return a {@link CompletionStage} which will complete with the result of {@code fn}
   * @see CompletionStage#handle(BiFunction)
   */
  public static <T, U> CompletionStage<U> thenComposeOrRecover(
      final CompletionStage<T> stage,
      final BiFunction<? super T, Throwable, ? extends CompletionStage<U>> fn) {
    final CompletableFuture<U> ret = new CompletableFuture<>();
    stage.whenComplete((t, throwable) -> {
      try {
        fn.apply(t, throwable).whenComplete((u, ex2) -> {
          if (ex2 != null) {
            ret.completeExceptionally(ex2);
          }
          ret.complete(u);
        });
      } catch (final Throwable e) {
        ret.completeExceptionally(e);
      }
    });
    return ret;
  }
}
