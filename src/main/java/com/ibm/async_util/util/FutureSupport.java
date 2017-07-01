package com.ibm.async_util.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class FutureSupport {
  private FutureSupport() {}

  private static final CompletableFuture<Void> VOID = CompletableFuture.completedFuture(null);

  /**
   * A common static instance to use instead of CompletableFuture.completedFuture(null)
   *
   * <p>This has a few advantages:
   *
   * <ul>
   *   <li>Depending on context, Futures.of(null) could either mean a {@code Future<Void>} or an
   *       {@literal Future<T>}. Using this method clearly indicates that we are returning a void
   *       future, not a normal future with a null result.
   *   <li>Immediately completed null futures are very common. Since they are final and static, we
   *       can just reuse a single object and save allocations
   * </ul>
   *
   * @return An immediately completed future of Void
   */
  public static CompletionStage<Void> voidFuture() {
    return VOID;
  }

  /**
   * Ignore the result of a future
   *
   * @param future
   * @return A future of Void when the input future completes
   */
  public static <T> CompletionStage<Void> voided(final CompletionStage<T> future) {
    return future.thenApply(ig -> null);
  }

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

  public static <T, R extends AsyncCloseable> CompletionStage<T> tryWithStagedClose(
      final CompletionStage<? extends R> resource,
      final Function<? super R, ? extends T> actionUnderResource) {
    return resource.thenCompose(
        r -> {
          try {
            T t = actionUnderResource.apply(r);
            return r.close().thenApply(ig -> t);
          } catch (Throwable e) {
            return r.close().handleAsync((t, ex) -> {
                throw new CompletionException(e);
            });
          }
        });
  }

  public static <T, R extends AsyncCloseable> CompletionStage<T> tryComposeWithStagedClose(
      final CompletionStage<? extends R> resource,
      final Function<? super R, ? extends CompletionStage<T>> actionUnderResource) {

    return resource.thenCompose(
        r ->
            actionUnderResource
                .apply(r)
                .thenApply(Either::<Throwable, T>right)
                .exceptionally(Either::<Throwable, T>left)
                .thenCompose(
                    either ->
                        r.close()
                            .thenApply(
                                ig ->
                                    either.fold(
                                        ex -> {
                                          throw new CompletionException(ex);
                                        },
                                        t -> t))));
  }
}
