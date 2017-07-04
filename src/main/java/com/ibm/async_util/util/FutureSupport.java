package com.ibm.async_util.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

public class FutureSupport {
  private FutureSupport() {}

  private static final CompletableFuture<Void> VOID = CompletableFuture.completedFuture(null);

  /**
   * Gets an already completed {@link CompletionStage} of Void. This common static instance can be
   * used as an alternative to {@code CompletableFuture.completedFuture(null)}
   *
   * <p>This has a few advantages:
   *
   * <ul>
   *   <li>Depending on context, CompletableFuture.completedFuture(null) could either mean a {@code
   *       CompletionStage<Void>} or an {@code CompletionStage<T>}. Using this method clearly
   *       indicates that we are returning a void future, not a T future with a null result.
   *   <li>Immediately completed null futures are very common. Since they are final and static, we
   *       can just reuse a single object and save allocations
   * </ul>
   *
   * @return An immediately completed {@link CompletionStage} of {@code Void}
   */
  public static CompletionStage<Void> voidFuture() {
    return VOID;
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
   * Creates a {@link CompletionStage} that is already completed exceptionally. This is the
   * exceptional analog of {@link CompletableFuture#completedFuture(Object)}.
   *
   * @param ex the exception that completes the returned stage
   * @return a {@link CompletionStage} that has already been completed exceptionally with {@code ex}
   */
  public static <T> CompletionStage<T> errorStage(final Throwable ex) {
    CompletableFuture<T> fut = new CompletableFuture<>();
    fut.completeExceptionally(ex);
    return fut;
  }

  /**
   * Performs an action with an asynchronously acquired {@link AutoCloseable auto closeable},
   * ensuring that the resource is {@link AutoCloseable#close() closed} after the action runs.
   * Similar to a try-with-resources block, the resource will be closed even if {@code
   * actionUnderResources} throws an exception.
   *
   * <p>The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   *   <li>{@code resource} completes exceptionally
   *   <li>{@code actionUnderResource} throws an exception
   *   <li>{@link AutoCloseable#close()} throws an exception
   * </ol>
   *
   * Of these cases, only 2 and 3 can happen simultaneously - in this cases, the exception thrown by
   * close will be added to the exception from {@code actionUnderResource} as a suppressed
   * exception. If {@link AutoCloseable#close()} throws a non-runtime exception, it will be wrapped
   * in a {@link CompletionException}.
   *
   * @param resource a {@link CompletionStage} that completes with an {@link AutoCloseable}
   * @param actionUnderResource an action to perform that uses result of {@code resource} to produce
   *     a value
   * @param <T> the result type of {@code actionUnderResource}
   * @param <R> the {@link AutoCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code actionUnderResource}
   *     or completes exceptionally
   */
  public static <T, R extends AutoCloseable> CompletionStage<T> tryWith(
      final CompletionStage<R> resource,
      final Function<? super R, ? extends T> actionUnderResource) {
    return resource.thenApply(
        r -> {
          try {
            try (R rtemp = r) {
              return actionUnderResource.apply(r);
            }
          } catch (RuntimeException rte) {
            throw rte;
          } catch (Throwable ex) {
            throw new CompletionException(ex);
          }
        });
  }

  /**
   * Performs an asynchronous action with an asynchronously acquired {@link AutoCloseable}, ensuring
   * that the resource is {@link AutoCloseable#close() closed} after the stage returned by the
   * action completes. Similar to a try-with-resources block, the resource will be closed even if
   * {@code actionUnderResources} throws an exception or if the returned stage completes
   * exceptionally.
   *
   * <p>The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   *   <li>{@code resource} completes exceptionally
   *   <li>{@code actionUnderResource} throws an exception
   *   <li>{@code actionUnderResource} returns an stage that completes exceptionally
   *   <li>{@link AutoCloseable#close()} throws an exception
   * </ol>
   *
   * Of these cases, (2 and 4) and (3 and 4) can happen simultaneously - in these cases, the
   * exception thrown by close will be added to the exception from {@code actionUnderResource} as a
   * suppressed exception. If {@link AutoCloseable#close()} throws a non-runtime exception, it will
   * be wrapped in a {@link CompletionException}.
   *
   * @param resource a {@link CompletionStage} that completes with an {@link AutoCloseable} which
   *     will be {{@link AutoCloseable#close()} closed} when the stage returned by {@code
   *     actionUnderResource} completes.
   * @param actionUnderResource an action to perform that uses result of {@code resource} to produce
   *     a new {@link CompletionStage}
   * @param <T> the type of the {@link CompletionStage} produced by {@code actionUnderResource}
   * @param <R> the {@link AutoCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code actionUnderResource}
   *     or completes exceptionally
   */
  public static <T, R extends AutoCloseable> CompletionStage<T> tryComposeWith(
      final CompletionStage<R> resource,
      final Function<? super R, ? extends CompletionStage<T>> actionUnderResource) {
    return resource.thenCompose(
        r -> {
          final CompletableFuture<T> ret = new CompletableFuture<>();
          try {
            actionUnderResource
                .apply(r)
                .whenComplete(
                    (t, actionEx) -> {
                      try {
                        r.close();
                        if (t != null) {
                          ret.complete(t);
                        } else {
                          ret.completeExceptionally(actionEx);
                        }
                      } catch (Exception closeEx) {
                        if (actionEx != null) {
                          actionEx.addSuppressed(closeEx);
                          ret.completeExceptionally(actionEx);
                        } else {
                          ret.completeExceptionally(closeEx);
                        }
                      }
                    });
          } catch (Throwable actionException) {
            try {
              r.close();
            } catch (Exception closeException) {
              actionException.addSuppressed(closeException);
            }
            ret.completeExceptionally(actionException);
          }
          return ret;
        });
  }

  /**
   * Uses the possibly exceptional result of {@code stage} to produce a new stage.
   *
   * <p>When {@code stage} completes, {@code fn} will be applied with the result of the stage (or
   * null if it completed exceptionally) and the exception from the stage (or null if it completed
   * normally) to produce a new stage. The returned stage will be completed with the outcome of the
   * stage produced by {@code fn}.
   *
   * <pre>{@code
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
   * }</pre>
   *
   * @param stage a {@link CompletionStage} that may complete exceptionally
   * @param fn a function that will run with the outcome of {@code stage} to produce a new {@link
   *     CompletionStage}
   * @param <T> the type {@code stage}
   * @param <U> the type of the returned {@link CompletionStage}
   * @return a {@link CompletionStage} which will complete with the result of {@code fn}
   * @see CompletionStage#handle(BiFunction)
   */
  public static <T, U> CompletionStage<U> thenComposeOrRecover(
      final CompletionStage<T> stage,
      final BiFunction<? super T, Throwable, ? extends CompletionStage<U>> fn) {
    final CompletableFuture<U> ret = new CompletableFuture<>();
    stage.whenComplete(
        (t, throwable) -> {
          try {
            fn.apply(t, throwable)
                .whenComplete(
                    (u, ex2) -> {
                      if (ex2 != null) {
                        ret.completeExceptionally(ex2);
                      }
                      ret.complete(u);
                    });
          } catch (Throwable e) {
            ret.completeExceptionally(e);
          }
        });
    return ret;
  }
}
