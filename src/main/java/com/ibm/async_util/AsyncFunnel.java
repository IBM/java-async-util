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

package com.ibm.async_util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;


/**
 * Used when many clients need the result of an expensive asynchronous operation but only one should
 * be ongoing at any given time. If {@link AsyncFunnel#doOrGet(Collapsible)} called while an
 * operation is ongoing, the future result of that ongoing task is returned
 * 
 * Note: doOrGet does not have a hard guarantee that when the future completed by doOrGet completes
 * a new call to doOrGet will generate a new future. (Usually, that will be the case though)
 */
public class AsyncFunnel<T> {
  private final AtomicReference<CompletionStage<T>> current = new AtomicReference<>(null);

  public CompletionStage<T> doOrGet(
      final Supplier<CompletionStage<T>> action) {
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
