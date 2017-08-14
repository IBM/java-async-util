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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import com.ibm.asyncutil.util.TestUtil.CompletableStage;

/**
 * A rudimentary implementation of {@link CompletionStage}. Used to validate CompletionStage
 * behavior against a class that isn't {@link CompletableFuture}
 */
public class SimpleCompletionStage<T> implements CompletableStage<T> {
  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<SimpleCompletionStage, Object> RESULT_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(SimpleCompletionStage.class, Object.class, "result");
  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<SimpleCompletionStage, Node> STACK_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(SimpleCompletionStage.class, Node.class, "stack");

  private static final Executor LOCAL = Runnable::run;
  private static final Executor ASYNC = CompletableFuture::runAsync;

  private static final Object NIL = new ExceptionalResult(null);

  private volatile Object result;
  @SuppressWarnings("unused") // used via STACK_UPDATER
  private volatile Node<T> stack;

  private static class Node<T> {
    Node<T> next;
    private final CompletionAction<T> action;

    public Node(final CompletionAction<T> action) {
      this.action = action;
    }
  }

  private interface CompletionAction<T> {
    void accept(Object res, T t, Throwable exc);
  }

  private static class ExceptionalResult {
    private final Throwable throwable;

    public ExceptionalResult(final Throwable throwable) {
      this.throwable = throwable;
    }
  }

  /**
   * @return true iff stage wasn't complete and is now complete with the given result
   */
  @Override
  public boolean complete(final T result) {
    return completeEncoded(result == null ? NIL : result);
  }

  /**
   * @return true iff stage wasn't complete and is now complete with the given exception
   */
  @Override
  public boolean completeExceptionally(final Throwable exception) {
    return completeEncoded(new ExceptionalResult(Objects.requireNonNull(exception)));
  }

  private boolean completeEncoded(final Object res) {
    if (RESULT_UPDATER.compareAndSet(this, null, res)) {
      popAllAndRun(res);
      return true;
    }
    return false;
  }

  @Override
  public boolean isDone() {
    return this.result != null;
  }

  private void push(final CompletionAction<T> action) {
    final Node<T> n = new Node<>(action);
    STACK_UPDATER.updateAndGet(this, head -> {
      @SuppressWarnings("unchecked")
      final Node<T> safeNode = head;
      n.next = safeNode;
      return n;
    });

    final Object res = this.result;
    if (res != null) {
      popAllAndRun(res);
    }
  }

  private void popAllAndRun(final Object res) {
    final T t;
    final Throwable exc;
    if (res instanceof ExceptionalResult) {
      t = null;
      exc = ((ExceptionalResult) res).throwable;
    } else {
      @SuppressWarnings("unchecked")
      final T safeT = (T) res;
      t = safeT;
      exc = null;
    }

    for (@SuppressWarnings("unchecked")
    Node<T> n = STACK_UPDATER.getAndSet(this, null); n != null; n = n.next) {
      n.action.accept(res, t, exc);
    }
  }

  private void runOrPush(final CompletionAction<T> action) {
    final Object res = this.result;
    if (res == null) {
      push(action);
    } else {
      if (res instanceof ExceptionalResult) {
        action.accept(res, null, ((ExceptionalResult) res).throwable);
      } else {
        @SuppressWarnings("unchecked")
        final T safeT = (T) res;
        action.accept(res, safeT, null);
      }
    }
  }

  private static <T> CompletionException wrapIfNecessary(final Throwable e) {
    return e instanceof CompletionException
        ? (CompletionException) e
        : new CompletionException(e);
  }

  @Override
  public <U> CompletionStage<U> thenApply(final Function<? super T, ? extends U> fn) {
    return thenApplyAsync(fn, LOCAL);
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
    return thenApplyAsync(fn, ASYNC);
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(final Function<? super T, ? extends U> fn,
      final Executor executor) {
    Objects.requireNonNull(fn);
    Objects.requireNonNull(executor);
    final SimpleCompletionStage<U> scs = new SimpleCompletionStage<>();
    runOrPush((res, t, exc) -> {
      try {
        executor.execute(() -> {
          if (exc == null) {
            try {
              scs.complete(fn.apply(t));
            } catch (final Throwable e) {
              scs.completeExceptionally(SimpleCompletionStage.wrapIfNecessary(e));
            }
          } else {
            scs.completeEncoded(res);
          }
        });
      } catch (final Throwable e) {
        scs.completeExceptionally(e);
      }
    });
    return scs;
  }

  @Override
  public CompletionStage<Void> thenAccept(final Consumer<? super T> action) {
    return thenAcceptAsync(action, LOCAL);
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(final Consumer<? super T> action) {
    return thenAcceptAsync(action, ASYNC);
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(final Consumer<? super T> action,
      final Executor executor) {
    Objects.requireNonNull(action);
    return thenApplyAsync(t -> {
      action.accept(t);
      return null;
    }, executor);
  }

  @Override
  public CompletionStage<Void> thenRun(final Runnable action) {
    return thenRunAsync(action, LOCAL);
  }

  @Override
  public CompletionStage<Void> thenRunAsync(final Runnable action) {
    return thenRunAsync(action, ASYNC);
  }

  @Override
  public CompletionStage<Void> thenRunAsync(final Runnable action, final Executor executor) {
    Objects.requireNonNull(action);
    return thenApplyAsync(t -> {
      action.run();
      return null;
    }, executor);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombine(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn) {
    return thenCombineAsync(other, fn, LOCAL);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn) {
    return thenCombineAsync(other, fn, ASYNC);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn, final Executor executor) {
    Objects.requireNonNull(fn);
    final SimpleCompletionStage<V> scs = new SimpleCompletionStage<>();
    other.whenCompleteAsync((u, uExc) -> {
      try {
        this.whenCompleteAsync((t, tExc) -> {
          if (uExc == null) {
            if (tExc == null) {
              try {
                scs.complete(fn.apply(t, u));
              } catch (final Throwable e) {
                scs.completeExceptionally(SimpleCompletionStage.wrapIfNecessary(e));
              }
            } else {
              scs.completeExceptionally(tExc);
            }
          } else {
            if (tExc != null) {
              uExc.addSuppressed(tExc);
            }
            scs.completeExceptionally(uExc);
          }
        }, executor);
      } catch (final Throwable e) {
        scs.completeExceptionally(e);
      }
    }, executor);
    return scs;
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action) {
    return thenAcceptBothAsync(other, action, LOCAL);
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action) {
    return thenAcceptBothAsync(other, action, ASYNC);
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action, final Executor executor) {
    Objects.requireNonNull(action);
    return thenCombineAsync(other, (t, u) -> {
      action.accept(t, u);
      return null;
    }, executor);
  }

  @Override
  public CompletionStage<Void> runAfterBoth(final CompletionStage<?> other, final Runnable action) {
    return runAfterBothAsync(other, action, LOCAL);
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(final CompletionStage<?> other,
      final Runnable action) {
    return runAfterBothAsync(other, action, ASYNC);
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(final CompletionStage<?> other,
      final Runnable action,
      final Executor executor) {
    Objects.requireNonNull(action);
    return thenCombineAsync(other, (t, u) -> {
      action.run();
      return null;
    }, executor);
  }

  @Override
  public <U> CompletionStage<U> applyToEither(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn) {
    return applyToEitherAsync(other, fn, LOCAL);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn) {
    return applyToEitherAsync(other, fn, ASYNC);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn, final Executor executor) {
    Objects.requireNonNull(fn);
    final SimpleCompletionStage<T> scs = new SimpleCompletionStage<>();

    runOrPush((res, t, exc) -> scs.completeEncoded(res));

    other.whenComplete((t, exc) -> {
      if (exc == null) {
        scs.complete(t);
      } else {
        scs.completeExceptionally(exc);
      }
    });

    return scs.thenApplyAsync(fn, executor);
  }

  @Override
  public CompletionStage<Void> acceptEither(final CompletionStage<? extends T> other,
      final Consumer<? super T> action) {
    return acceptEitherAsync(other, action, LOCAL);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
      final Consumer<? super T> action) {
    return acceptEitherAsync(other, action, ASYNC);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
      final Consumer<? super T> action, final Executor executor) {
    Objects.requireNonNull(action);
    return applyToEitherAsync(other, t -> {
      action.accept(t);
      return null;
    }, executor);
  }

  @Override
  public CompletionStage<Void> runAfterEither(final CompletionStage<?> other,
      final Runnable action) {
    return runAfterEitherAsync(other, action, LOCAL);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(final CompletionStage<?> other,
      final Runnable action) {
    return runAfterEitherAsync(other, action, ASYNC);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(final CompletionStage<?> other,
      final Runnable action,
      final Executor executor) {
    Objects.requireNonNull(action);

    @SuppressWarnings("unchecked")
    final CompletionStage<T> safeStage = (CompletionStage<T>) other;

    return applyToEitherAsync(safeStage, t -> {
      action.run();
      return null;
    }, executor);
  }

  @Override
  public <U> CompletionStage<U> thenCompose(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    return thenComposeAsync(fn, LOCAL);
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    return thenComposeAsync(fn, ASYNC);
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn, final Executor executor) {
    Objects.requireNonNull(fn);
    Objects.requireNonNull(executor);
    final SimpleCompletionStage<U> scs = new SimpleCompletionStage<>();
    runOrPush((res, t, tExc) -> {
      if (tExc == null) {
        try {
          executor.execute(() -> {
            try {
              fn.apply(t).whenComplete((u, uExc) -> {
                if (uExc == null) {
                  scs.complete(u);
                } else {
                  scs.completeExceptionally(uExc);
                }
              });
            } catch (final Throwable e) {
              scs.completeExceptionally(SimpleCompletionStage.wrapIfNecessary(e));
            }
          });
        } catch (final Throwable e) {
          scs.completeExceptionally(e);
        }
      } else {
        scs.completeEncoded(res);
      }
    });
    return scs;
  }

  @Override
  public CompletionStage<T> exceptionally(final Function<Throwable, ? extends T> fn) {
    Objects.requireNonNull(fn);
    final SimpleCompletionStage<T> scs = new SimpleCompletionStage<>();
    runOrPush((res, t, exc) -> {
      if (exc == null) {
        scs.completeEncoded(res);
      } else {
        try {
          scs.complete(fn.apply(exc));
        } catch (final Throwable e) {
          scs.completeExceptionally(SimpleCompletionStage.wrapIfNecessary(e));
        }
      }
    });
    return scs;
  }

  @Override
  public CompletionStage<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
    return whenCompleteAsync(action, LOCAL);
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(
      final BiConsumer<? super T, ? super Throwable> action) {
    return whenCompleteAsync(action, ASYNC);
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action,
      final Executor executor) {
    Objects.requireNonNull(action);
    final SimpleCompletionStage<T> scs = new SimpleCompletionStage<>();
    runOrPush((res, t, exc) -> {
      try {
        executor.execute(() -> {
          try {
            action.accept(t, exc);
          } catch (final Throwable e) {
            if (exc == null) {
              scs.completeExceptionally(SimpleCompletionStage.wrapIfNecessary(e));
              return;
            }
            exc.addSuppressed(e);
          }
          scs.completeEncoded(res);
        });
      } catch (final Throwable e) {
        scs.completeExceptionally(e);
      }
    });
    return scs;
  }

  @Override
  public <U> CompletionStage<U> handle(final BiFunction<? super T, Throwable, ? extends U> fn) {
    return handleAsync(fn, LOCAL);
  }

  @Override
  public <U> CompletionStage<U> handleAsync(
      final BiFunction<? super T, Throwable, ? extends U> fn) {
    return handleAsync(fn, ASYNC);
  }

  @Override
  public <U> CompletionStage<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn,
      final Executor executor) {
    Objects.requireNonNull(fn);
    final SimpleCompletionStage<U> scs = new SimpleCompletionStage<>();
    whenCompleteAsync((t, exc) -> {
      try {
        scs.complete(fn.apply(t, exc));
      } catch (final Throwable e) {
        scs.completeExceptionally(SimpleCompletionStage.wrapIfNecessary(e));
      }
    }, executor);
    return scs;
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    final CompletableFuture<T> cf = new CompletableFuture<>();
    runOrPush((res, t, exc) -> {
      if (exc == null) {
        cf.complete(t);
      } else {
        cf.completeExceptionally(exc);
      }
    });
    return cf;
  }
}
