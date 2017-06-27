package com.ibm.async_util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class provides methods for asynchronous "stack unrolling". When working with futures, it's
 * often desirable to keep producing futures until some condition is met (like a while loop).
 * Because continuations are asynchronous, it's usually easiest to do this with a recursive
 * approach:
 *
 * <pre>{@code
 * Future<Integer> getNextNumber();
 *
 * Future<Integer> getFirstOddNumber(int current) {
 *   if (current % 2 != 0)
 *     // found odd number
 *     return Futures.of(current);
 *   else
 *     // get the next number and recurse
 *     return getNextNumber().flatMap(next -> getFirstOddNumber(next));
 * }
 * }</pre>
 *
 * The problem with this is that if the implementation of getNextNumber is synchronous:
 *
 * <pre>{@code
 * Future<Integer> getNextNumber() {
 *   return Futures.of(random.nextInt());
 * }
 * }</pre>
 *
 * then getFirstOddNumber can easily StackOverflow. This situation often happens when a cache is put
 * under an async API, and all the values are cached and returned immediately.
 *
 * <p>The methods on this class ensure that the stack doesn't blow up - if multiple calls happen on
 * the same thread they are queued and run in a while loop. You could write the previous example
 * like this:
 *
 * <pre>{@code
 * Future<Integer> getFirstOddNumber(int initial) {
 *   return StackUnroller.asyncWhile(i -> i % 2 == 0, i -> getNextNumber(), initial);
 * }
 * }</pre>
 *
 * @see AsyncIterator
 */
public final class StackUnroller {

  private StackUnroller() {}

  private static class StackUnrollerInternal<T> extends CompletableFuture<T> {

    private final Predicate<T> shouldContinue;
    private final Function<T, CompletionStage<T>> f;

    private StackUnrollerInternal(
        final Predicate<T> shouldContinue,
        final Function<T, CompletionStage<T>> f,
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
          if (this.shouldContinue.test(c)) {
            try {
              this.f
                  .apply(c)
                  .whenComplete(
                      (next, ex) -> {
                        if (ex != null) {
                          this.completeExceptionally(ex);
                        } else {
                          unroll(next, currentThread, currentPassBack);
                        }
                      });
            } catch (Exception e) {
              this.completeExceptionally(e);
              return;
            }
          } else {
            this.complete(c);
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
   * Like {@link StackUnroller#asyncWhile(Predicate, Function, Object)}, except the body is run
   * first on the initial value, without first testing via shouldContinue. f is guaranteed to be
   * called at least once.
   *
   * @param f
   * @param initialValue
   * @param shouldContinue
   * @return A future that contains the first value other than initialValue such that shouldContinue
   *     returns false
   */
  public static <T> CompletionStage<T> asyncDoWhile(
      final Function<T, CompletionStage<T>> f,
      final T initialValue,
      final Predicate<T> shouldContinue) {
    return f.apply(initialValue).thenCompose(t -> StackUnroller.asyncWhile(shouldContinue, f, t));
  }

  /**
   * Repeatedly apply an asynchronous function f to a value, but unroll the stack to avoid
   * stackoverflows. (Basically async tail call optimization)
   *
   * <p>Effectively produces f(seed).flatMap(f).flatMap(f)... .flatMap(f) until an value fails the
   * predicate. Note that predicate will be applied on seed (like a while loop, the initial value is
   * tested). For a do/while style see {@link #asyncDoWhile(Function, Object, Predicate)}.
   *
   * @param shouldContinue - Whether we should continue
   * @param f - the loop body, producing the next result
   * @param initialValue
   * @return A future that contains the first value t such that shouldContinue.test(T) == false
   */
  public static <T> CompletionStage<T> asyncWhile(
      final Predicate<T> shouldContinue,
      final Function<T, CompletionStage<T>> f,
      final T initialValue) {
    return new StackUnrollerInternal<>(shouldContinue, f, initialValue);
  }

  /**
   * Loop body simply returns whether iteration should continue. True means continue, False means
   * break.
   *
   * @param f a way to generate a future that indicates whether iteration should continue
   * @return A future that is complete when iteration has completed
   */
  public static CompletionStage<Void> asyncWhile(final Supplier<CompletionStage<Boolean>> f) {
    return FutureSupport.voided(StackUnroller.asyncWhile(b -> b, b -> f.get(), true));
  }
}
