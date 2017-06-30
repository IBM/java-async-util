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

package com.ibm.async_util.iteration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;


/**
 * Enforces that a single asynchronous computation occurs at a time, allowing others to observe it.
 * 
 * Used when many clients need the result of an expensive asynchronous operation but only one should
 * be ongoing at any given time. If {@link AsyncFunnel#doOrGet} is called while an operation is
 * ongoing, the future result of that ongoing task is returned. After the operation completes, a new
 * task can be created.
 * 
 */
public class AsyncFunnel<T> {
  private final AtomicReference<CompletionStage<T>> current = new AtomicReference<>(null);

  /**
   * Runs the provided action, or returns an existing future if the action is already running.
   * 
   * This method does not have a hard guarantee that when the future completed by doOrGet completes
   * a new call to doOrGet will generate a new future. (Usually, that will be the case though)
   * 
   * @param action Supplier of a future of T that only runs if no action is currently running
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
    } while (!this.current.compareAndSet(null, newFuture = new CompletableFuture<T>()));

    final CompletableFuture<T> finalRef = newFuture;
    return action.get().whenComplete((t, ex) -> {
      current.set(null);
      if (t != null) {
        finalRef.complete(t);
      } else {
        finalRef.completeExceptionally(ex);
      }

    });

  }
}
