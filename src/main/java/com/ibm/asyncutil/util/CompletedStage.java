/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link CompletionStage} which is already complete at initialization. Used by
 * {@link StageSupport#completedStage(Object)} and {@link StageSupport#exceptionalStage(Throwable)}
 * <p>
 * The default execution facility used by this stage implementation is effectively the same as the
 * one in {@link CompletableFuture} (by the use of {@link CompletableFuture#runAsync(Runnable)} and
 * similar methods) except when calling compound methods involving another stage (like
 * {@link #runAfterBothAsync(CompletionStage, Runnable)}) which use the other stage's corresponding
 * Async method to execute the computation
 */
class CompletedStage<T> implements CompletionStage<T> {
  static final CompletedStage<Void> VOID = CompletedStage.of(null);

  private final T result;
  private final Throwable exception;

  private CompletedStage(final T result, final Throwable exception) {
    this.result = result;
    this.exception = exception;
  }

  /**
   * return this exceptional stage as if its value type was U
   */
  @SuppressWarnings("unchecked")
  private <U> CompletedStage<U> typedException() {
    assert this.exception != null;
    return (CompletedStage<U>) this;
  }

  /**
   * Initialize a completed stage with a (non-exceptional) value
   */
  static <T> CompletedStage<T> of(final T t) {
    return new CompletedStage<>(t, null);
  }

  /**
   * Initialize a completed stage with an exception
   */
  static <T> CompletedStage<T> exception(final Throwable e) {
    return new CompletedStage<>(null, Objects.requireNonNull(e));
  }

  /**
   * Initialize a completed stage with a {@link CompletionException} (wrapping the given throwable
   * in one if it is not already of the type). As outlined in the {@link CompletionStage} interface,
   * this should be used when dependent actions throw exceptions during computation
   */
  private static <T> CompletedStage<T> completionException(final Throwable e) {
    return CompletedStage.exception(CompletedStage.wrapIfNecessary(e));
  }

  private static <T> CompletionException wrapIfNecessary(final Throwable e) {
    return e instanceof CompletionException
        ? (CompletionException) e
        : new CompletionException(e);
  }

  @Override
  public <U> CompletionStage<U> thenApply(final Function<? super T, ? extends U> fn) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      final U u;
      try {
        u = fn.apply(this.result);
      } catch (final Throwable e) {
        return CompletedStage.completionException(e);
      }
      return CompletedStage.of(u);
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      return CompletableFuture.supplyAsync(() -> fn.apply(this.result));
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(final Function<? super T, ? extends U> fn,
      final Executor executor) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      return CompletableFuture.supplyAsync(() -> fn.apply(this.result), executor);
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> thenAccept(final Consumer<? super T> action) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      try {
        action.accept(this.result);
      } catch (final Throwable e) {
        return CompletedStage.completionException(e);
      }
      return VOID;
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(final Consumer<? super T> action) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      return CompletableFuture.runAsync(() -> action.accept(this.result));
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(final Consumer<? super T> action,
      final Executor executor) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      return CompletableFuture.runAsync(() -> action.accept(this.result), executor);
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> thenRun(final Runnable action) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      try {
        action.run();
      } catch (final Throwable e) {
        return CompletedStage.completionException(e);
      }
      return VOID;
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> thenRunAsync(final Runnable action) {
    if (this.exception == null) {
      return CompletableFuture.runAsync(action);
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> thenRunAsync(final Runnable action, final Executor executor) {
    if (this.exception == null) {
      return CompletableFuture.runAsync(action, executor);
    }
    return typedException();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombine(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      return other.thenApply(u -> fn.apply(this.result, u));
    }
    return typedException();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      return other.thenApplyAsync(u -> fn.apply(this.result, u));
    }
    return typedException();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn, final Executor executor) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      return other.thenApplyAsync(u -> fn.apply(this.result, u), executor);
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      return other.thenAccept(u -> action.accept(this.result, u));
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      return other.thenAcceptAsync(u -> action.accept(this.result, u));
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action, final Executor executor) {
    Objects.requireNonNull(action);
    if (this.exception == null) {
      return other.thenAcceptAsync(u -> action.accept(this.result, u), executor);
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> runAfterBoth(final CompletionStage<?> other, final Runnable action) {
    if (this.exception == null) {
      return other.thenRun(action);
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(final CompletionStage<?> other,
      final Runnable action) {
    if (this.exception == null) {
      return other.thenRunAsync(action);
    }
    return typedException();
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(final CompletionStage<?> other,
      final Runnable action,
      final Executor executor) {
    if (this.exception == null) {
      return other.thenRunAsync(action, executor);
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<U> applyToEither(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn) {
    return thenApply(fn);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn) {
    return thenApplyAsync(fn);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn, final Executor executor) {
    return thenApplyAsync(fn, executor);
  }

  @Override
  public CompletionStage<Void> acceptEither(final CompletionStage<? extends T> other,
      final Consumer<? super T> action) {
    return thenAccept(action);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
      final Consumer<? super T> action) {
    return thenAcceptAsync(action);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
      final Consumer<? super T> action, final Executor executor) {
    return thenAcceptAsync(action, executor);
  }

  @Override
  public CompletionStage<Void> runAfterEither(final CompletionStage<?> other,
      final Runnable action) {
    return thenRun(action);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(final CompletionStage<?> other,
      final Runnable action) {
    return thenRunAsync(action);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(final CompletionStage<?> other,
      final Runnable action,
      final Executor executor) {
    return thenRunAsync(action, executor);
  }

  @Override
  public <U> CompletionStage<U> thenCompose(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      try {
        return fn.apply(this.result);
      } catch (final Throwable e) {
        return CompletedStage.completionException(e);
      }
    }
    return typedException();
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    return thenComposeAsyncInternal(fn, CompletableFuture::runAsync);
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn, final Executor executor) {
    return thenComposeAsyncInternal(fn, Objects.requireNonNull(executor));
  }

  private <U> CompletionStage<U> thenComposeAsyncInternal(
      final Function<? super T, ? extends CompletionStage<U>> fn, final Executor executor) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      final CompletableFuture<U> ret = new CompletableFuture<>();
      executor.execute(() -> {
        try {
          fn.apply(this.result)
              .whenComplete((u, exc) -> {
                if (exc == null) {
                  ret.complete(u);
                } else {
                  ret.completeExceptionally(exc);
                }
              });
        } catch (final Throwable e) {
          ret.completeExceptionally(CompletedStage.wrapIfNecessary(e));
        }
      });
      return ret;
    }
    return typedException();
  }

  @Override
  public CompletionStage<T> exceptionally(final Function<Throwable, ? extends T> fn) {
    Objects.requireNonNull(fn);
    if (this.exception == null) {
      return this;
    }

    final T t;
    try {
      t = fn.apply(this.exception);
    } catch (final Throwable e) {
      return CompletedStage.completionException(e);
    }
    return CompletedStage.of(t);
  }

  @Override
  public CompletionStage<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
    Objects.requireNonNull(action);
    try {
      action.accept(this.result, this.exception);
    } catch (final Throwable e) {
      if (this.exception == null) {
        return CompletedStage.completionException(e);
      }
    }
    return this;
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(
      final BiConsumer<? super T, ? super Throwable> action) {
    return whenCompleteAsyncInternal(action, CompletableFuture::runAsync);
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action,
      final Executor executor) {
    return whenCompleteAsyncInternal(action, Objects.requireNonNull(executor));
  }

  private CompletionStage<T> whenCompleteAsyncInternal(
      final BiConsumer<? super T, ? super Throwable> action,
      final Executor executor) {
    Objects.requireNonNull(action);
    final CompletableFuture<T> ret = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        action.accept(this.result, this.exception);
      } catch (final Throwable e) {
        ret.completeExceptionally(this.exception == null ? e : this.exception);
        return;
      }

      if (this.exception == null) {
        ret.complete(this.result);
      } else {
        ret.completeExceptionally(this.exception);
      }
    });
    return ret;
  }

  @Override
  public <U> CompletionStage<U> handle(final BiFunction<? super T, Throwable, ? extends U> fn) {
    Objects.requireNonNull(fn);
    final U u;
    try {
      u = fn.apply(this.result, this.exception);
    } catch (final Throwable e) {
      return CompletedStage.completionException(e);
    }
    return CompletedStage.of(u);
  }

  @Override
  public <U> CompletionStage<U> handleAsync(
      final BiFunction<? super T, Throwable, ? extends U> fn) {
    Objects.requireNonNull(fn);
    return CompletableFuture.supplyAsync(() -> fn.apply(this.result, this.exception));
  }

  @Override
  public <U> CompletionStage<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn,
      final Executor executor) {
    Objects.requireNonNull(fn);
    return CompletableFuture.supplyAsync(() -> fn.apply(this.result, this.exception), executor);
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    if (this.exception == null) {
      return CompletableFuture.completedFuture(this.result);
    }

    final CompletableFuture<T> ret = new CompletableFuture<>();
    ret.completeExceptionally(this.exception);
    return ret;
  }
}
