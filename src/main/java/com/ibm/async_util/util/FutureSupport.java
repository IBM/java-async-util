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
   * used as an alternative to CompletableFuture.completedFuture(null)
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
   * @return An immediately completed {@link CompletionStage} of Void
   */
  public static CompletionStage<Void> voidFuture() {
    return VOID;
  }

  /**
   * Creates a {@link CompletionStage} that completes when {@code stage} completes but ignores the
   * result
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

  public static <T, R extends AutoCloseable> CompletionStage<T> tryWith(
      final CompletionStage<? extends R> resource,
      final Function<? super R, ? extends T> actionUnderResource) {
    return resource.thenApply(
        r -> {
          try {
            try (R rtemp = r) {
              return actionUnderResource.apply(r);
            }
          } catch (Throwable e) {
            throw new CompletionException(e);
          }
        });
  }

  public static <T, R extends AutoCloseable> CompletionStage<T> tryComposeWith(
      final CompletionStage<? extends R> resource,
      final Function<? super R, ? extends CompletionStage<T>> actionUnderResource) {
    return resource.thenCompose(
        r ->
            actionUnderResource
                .apply(r)
                .whenComplete(
                    (t, ex) -> {
                      try {
                        r.close();
                      } catch (Throwable e) {
                        throw new CompletionException(e);
                      }
                    }));
  }

    public static <T, U> CompletionStage<U> thenComposeOrRecover(final CompletionStage<T> stage, final BiFunction<T, Throwable, CompletionStage<U>> fn) {
    final CompletableFuture<U> ret = new CompletableFuture<>();
    stage.whenComplete((t, throwable) -> {
        try {
            fn.apply(t, throwable).whenComplete((u, ex2) -> {
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
