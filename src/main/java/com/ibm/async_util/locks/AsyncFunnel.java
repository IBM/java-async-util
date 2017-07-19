//
// (C) Copyright IBM Corp. 2005 All Rights Reserved.
//
// Contact Information:
//
// IBM Corporation
// Legal Department
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// END-OF-HEADER
//
// -----------------------
// @author: rkhadiwala
//
// Date: Aug 4, 2015
// ---------------------

package com.ibm.async_util.locks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Enforces that a single asynchronous computation occurs at a time, allowing others to observe it.
 *
 * <p>
 * Used when many clients need the result of an expensive asynchronous operation but only one should
 * be ongoing at any given time. If {@link AsyncFunnel#doOrGet} is called while an operation is
 * ongoing, the future result of that ongoing task is returned. After the operation completes, a new
 * task can be created.
 *
 * <p>
 * For example, imagine we have many threads that will perform a computation on a potentially cached
 * value. If the value is not in the cache, they will have to do an expensive operation to populate
 * the cache. Many threads may discover the value needs to be computed at the same time, but only
 * one thread needs to actually compute it.
 *
 * <pre>
 * {@code
 * AsyncFunnel readFunnel = new AsyncFunnel();
 * for (int i = 0; i < numThreads; i++) {
 *    CompletableFuture.supplyAsync(() -> {
 *
 *        val = readFromCache();
 *        if (val != null) {
 *          return FutureSupport.completedStage(val);
 *        }
 *        // populate the cache if we're first, or just listen for the result on the thread
 *        // already doing the computation if we're not
 *        return readFunnel.doOrGet(() -> {
 *            return expensiveOperation().thenApply(computedVal -> {
 *                updateCache(readVal);
 *                return readVal;
 *            });
 *        });
 *    })
 * }
 * }
 * </pre>
 */
public class AsyncFunnel<T> {
  private final AtomicReference<CompletionStage<T>> current = new AtomicReference<>(null);

  /**
   * Runs the provided action, or returns an existing {@link CompletionStage} if an action is
   * already running. After the returned stage is completed, the first subsequent doOrGet call will
   * start a new action
   *
   * @param action Supplier of a {@link CompletionStage} of T that only runs if no action is
   *        currently running
   * @return A {@link CompletionStage} produced by <code> action </code> or one that was previously
   *         produced
   */
  public CompletionStage<T> doOrGet(final Supplier<CompletionStage<T>> action) {
    CompletableFuture<T> newFuture;
    do {
      final CompletionStage<T> oldFuture = this.current.get();
      if (oldFuture != null) {
        return oldFuture;
      }
    } while (!this.current.compareAndSet(null, newFuture = new CompletableFuture<>()));

    final CompletableFuture<T> finalRef = newFuture;
    return action
        .get()
        .whenComplete(
            (t, ex) -> {
              this.current.set(null);
              if (ex != null) {
                finalRef.completeExceptionally(ex);
              } else {
                finalRef.complete(t);
              }
            });
  }
}
