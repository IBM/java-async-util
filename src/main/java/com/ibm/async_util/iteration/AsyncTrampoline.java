package com.ibm.async_util.iteration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.ibm.async_util.util.FutureSupport;

/**
 * Static methods for asynchronous looping procedures without blowing the stack.
 *
 * <p>
 * When working with {@link CompletionStage}, it's often desirable to have a loop like construct
 * which keeps producing stages until some condition is met. Because continuations are asynchronous,
 * it's usually easiest to do this with a recursive approach:
 *
 * <pre>
 * {@code
 * CompletionStage<Integer> getNextNumber();
 *
 * CompletionStage<Integer> getFirstOddNumber(int current) {
 *   if (current % 2 != 0)
 *     // found odd number
 *     return CompletableFuture.of(current);
 *   else
 *     // get the next number and recurse
 *     return getNextNumber().thenCompose(next -> getFirstOddNumber(next));
 * }
 * }
 * </pre>
 *
 * The problem with this is that if the implementation of getNextNumber happens to be synchronous
 *
 * <pre>
 * {@code
 * CompletionStage<Integer> getNextNumber() {
 *   return CompletableFuture.completedFuture(random.nextInt());
 * }
 * }
 * </pre>
 *
 * then getFirstOddNumber can easily cause a stack overflow. This situation often happens when a
 * cache is put under an async API, and all the values are cached and returned immediately. This
 * could be avoided by scheduling the recursive calls back to a thread pool using
 * {@link CompletionStage#thenComposeAsync}, however the overhead of the thread pool submissions may
 * be high and may cause unnecessary context switching.
 *
 * <p>
 * The methods on this class ensure that the stack doesn't blow up - if multiple calls happen on the
 * same thread they are queued and run in a loop. You could write the previous example like this:
 *
 * <pre>
 * {@code
 * CompletionStage<Integer> getFirstOddNumber(int initial) {
 *   return AsyncTrampoline.asyncWhile(
 *    i -> i % 2 == 0,
 *    i -> getNextNumber(),
 *    initial);
 * }
 * }
 * </pre>
 *
 * Though this class provides efficient methods for a few loop patterns, many are better represented
 * by the more expressive API available on {@link AsyncIterator}, which is also stack safe. For
 * example, the preceding snippet can be expressed as {@code
 * AsyncIterator.generate(this::getNextNumber).find(i -> i % 2 != 0)}
 *
 * @see AsyncIterator
 */
public final class AsyncTrampoline {

  private AsyncTrampoline() {}

  private static class TrampolineInternal<T> extends CompletableFuture<T> {

    private final Predicate<? super T> shouldContinue;
    private final Function<? super T, ? extends CompletionStage<T>> f;

    private TrampolineInternal(
        final Predicate<? super T> shouldContinue,
        final Function<? super T, ? extends CompletionStage<T>> f,
        final T initialValue) {
      this.shouldContinue = shouldContinue;
      this.f = f;
      unroll(initialValue, null, null);
    }

    private void unroll(
        final T completed, final Thread previousThread, final PassBack previousPassBack) {
      final Thread currentThread = Thread.currentThread();

      // we need to track termination in case f queues the future and currentThread later picks it
      // up, leading to sequential runs from the same thread but different stacks
      if (currentThread.equals(previousThread) && previousPassBack.isRunning) {
        previousPassBack.item = completed;
      } else {
        final PassBack currentPassBack = new PassBack();
        T c = completed;
        do {
          try {
            if (this.shouldContinue.test(c)) {
              this.f.apply(c).whenComplete((next, ex) -> {
                if (ex != null) {
                  completeExceptionally(ex);
                } else {
                  unroll(next, currentThread, currentPassBack);
                }
              });
            } else {
              complete(c);
              return;
            }
          } catch (final Throwable e) {
            completeExceptionally(e);
            return;
          }
        } while ((c = currentPassBack.poll()) != null);
        currentPassBack.isRunning = false;
      }
    }

    private class PassBack {
      boolean isRunning = true;
      T item = null;

      public T poll() {
        final T c = this.item;
        this.item = null;
        return c;
      }
    }
  }

  /**
   * Repeatedly applies an asynchronous function {@code fn} to a value until {@code shouldContinue}
   * returns {@code false}. The asynchronous equivalent of
   *
   * <pre>
   * {@code
   * T loop(Predicate shouldContinue, Function fn, T initialValue) {
   *   T t = initialValue;
   *   while (shouldContinue.test(t)) {
   *     t = fn.apply(t);
   *   }
   *   return t;
   * }
   * }
   * </pre>
   *
   * <p>
   * Effectively produces {@code fn(seed).thenCompose(fn).thenCompose(fn)... .thenCompose(fn)} until
   * an value fails the predicate. Note that predicate will be applied on seed (like a while loop,
   * the initial value is tested). For a do/while style see
   * {@link #asyncDoWhile(Function, Object, Predicate)}. If the predicate or fn throw an exception,
   * or the {@link CompletionStage} returned by fn completes exceptionally, iteration will stop and
   * an exceptional stage will be returned.
   *
   * @param shouldContinue a predicate which will be applied to every intermediate T value
   *        (including the {@code initialValue}) until it fails and looping stops.
   * @param fn the function for the loop body which produces a new {@link CompletionStage} based on
   *        the result of the previous iteration.
   * @param initialValue the value that will initially be passed to {@code fn}, it will also be
   *        initially tested by {@code shouldContinue}
   * @param <T> the type of elements produced by the loop
   * @return a {@link CompletionStage} that completes with the first value t such that {@code
   *     shouldContinue.test(T) == false}, or with an exception if one was thrown.
   */
  public static <T> CompletionStage<T> asyncWhile(
      final Predicate<? super T> shouldContinue,
      final Function<? super T, ? extends CompletionStage<T>> fn,
      final T initialValue) {
    return new TrampolineInternal<>(shouldContinue, fn, initialValue);
  }

  /**
   * Repeatedly use the function {@code fn} to produce a {@link CompletionStage} of a boolean,
   * stopping when then boolean is {@code false}. The asynchronous equivalent of {@code
   * while(fn.get());}. Generally, the function fn must perform some side effect for this method to
   * be useful. If the {@code fn} throws or produces an exceptional {@link CompletionStage}, an
   * exceptional stage will be returned.
   *
   * @param fn a {@link Supplier} of a {@link CompletionStage} that indicates whether iteration
   *        should continue
   * @return a {@link CompletionStage} that is complete when a stage produced by {@code fn} has
   *         returned {@code false}, or with an exception if one was thrown
   */
  public static CompletionStage<Void> asyncWhile(
      final Supplier<? extends CompletionStage<Boolean>> fn) {
    return FutureSupport.voided(AsyncTrampoline.asyncWhile(b -> b, b -> fn.get(), true));
  }

  /**
   * Repeatedly applies an asynchronous function {@code fn} to a value until {@code shouldContinue}
   * returns {@code false}, unconditionally applying fn to {@code initialValue} on the first
   * iteration. The asynchronous equivalent of
   *
   * <pre>
   * {@code
   * {
   *    T t = initialValue;
   *    do {
   *         t = fn.apply(t);
   *    } while(shouldContinue.test(t));
   *    return t;
   * }
   * }
   * </pre>
   *
   * <p>
   * Effectively produces {@code fn(seed).thenCompose(fn).thenCompose(fn)... .thenCompose(fn)} until
   * an value fails the predicate. Note that predicate will be applied on seed (like a while loop,
   *
   * <p>
   * Effectively produces fn(seed).then(fn).flatMap(fn)... .flatMap(fn) until an value fails the
   * predicate. Note that predicate will not be applied to {@code initialValue}, for a {@code while}
   * style loop see {@link #asyncWhile(Predicate, Function, Object)} . If the predicate or fn throw
   * an exception, or the {@link CompletionStage} returned by fn completes exceptionally, iteration
   * will stop and an exceptional stage will be returned.
   *
   * @param shouldContinue a predicate which will be applied to intermediate T value other than the
   *        {@code initialValue} the in until it fails and looping stops.
   * @param fn the function for the loop body which produces a new {@link CompletionStage} based on
   *        the result of the previous iteration.
   * @param initialValue the value that will initially be passed to {@code fn}, it will not be
   *        initially tested by {@code shouldContinue}
   * @param <T> the type of elements produced by the loop
   * @return a {@link CompletionStage} that completes with the first value t such that {@code
   *     shouldContinue.test(T) == false}, or with an exception if one was thrown.
   */
  public static <T> CompletionStage<T> asyncDoWhile(
      final Function<? super T, ? extends CompletionStage<T>> fn,
      final T initialValue,
      final Predicate<? super T> shouldContinue) {
    return fn.apply(initialValue)
        .thenCompose(t -> AsyncTrampoline.asyncWhile(shouldContinue, fn, t));
  }
}
