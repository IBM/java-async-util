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
// @author: renar
//
// Date: Aug 26, 2015
// ---------------------

package com.ibm.async_util.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * An object that may hold resources that must be explicitly released, where the release may be
 * performed asynchronously.
 *
 * <p>Examples of such resources are manually managed memory, open file handles, socket descriptors
 * etc. While similar to {@link AutoCloseable}, this interface should be used when the resource
 * release operation may possibly be async. For example, if an object is thread-safe and has many
 * consumers, an implementation may require all current ongoing operations to complete before
 * resources are relinquished. A common way to implement this pattern for a thread-safe object with
 * asynchronous methods is by using an {@link com.ibm.async_util.locks.ObservableEpoch}.
 *
 * <p>May be used with the methods {@link AsyncCloseable#tryWith(AsyncCloseable, Function)}, {@link
 * AsyncCloseable#tryComposeWith(AsyncCloseable, Function)} to emulate the behavior of a try with
 * resources block.
 */
@FunctionalInterface
public interface AsyncCloseable {
  /**
   * Relinquishes any resources associated with this object.
   *
   * @return a {@link CompletionStage} that completes when all resources associated with this object
   *     have been released, or with an exception if the resources cannot be released.
   */
  CompletionStage<Void> close();

  /**
   * Performs an asynchronous action with an {@link AsyncCloseable} resource, ensuring that the
   * resource is {@link AsyncCloseable#close()} closed} after the stage returned by the action
   * completes. The returned stage will complete when the stage returned by the close has completed.
   * Similar to a try-with-resources block, the resource will be closed even if {@code
   * actionUnderResources} throws an exception or if its returned stage completes exceptionally.
   *
   * <p>The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   *   <li>{@code actionUnderResource} throws an exception
   *   <li>{@code actionUnderResource} returns a stage that completes exceptionally
   *   <li>{@link AutoCloseable#close()} throws an exception
   *   <li>{@link AutoCloseable#close()} returns a stage that completes exceptionally
   * </ol>
   *
   * If the action produces an exception (cases 1 and 2) <b> and </b> {@link #close()} produces an
   * exception (cases 3 and 4) the exception produced by close will be added to the exception from
   * {@code actionUnderResource} as a suppressed exception.
   *
   * @param resource an {@link AsyncCloseable} which will be {{@link #close()} closed} when the
   *     stage returned by {@code actionUnderResource} completes.
   * @param actionUnderResource an action to perform that uses {@code resource} to produce a new
   *     {@link CompletionStage}
   * @param <T> the type of the {@link CompletionStage} produced by {@code actionUnderResource}
   * @param <R> the {@link AsyncCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code actionUnderResource}
   *     after close has completed or completes exceptionally
   */
  static <T, R extends AsyncCloseable> CompletionStage<T> tryComposeWith(
      final R resource,
      final Function<? super R, ? extends CompletionStage<T>> actionUnderResource) {
    try {
      final CompletionStage<T> stage = actionUnderResource.apply(resource);
      final CompletableFuture<T> ret = new CompletableFuture<>();
      stage.whenComplete(
          (t, ex) -> {
            try {
              resource
                  .close()
                  .whenComplete(
                      (ig, closeEx) -> {
                        if (ex != null) {
                          if (closeEx != null) {
                            ex.addSuppressed(closeEx);
                          }
                          ret.completeExceptionally(ex);
                        } else if (closeEx != null) {
                          ret.completeExceptionally(closeEx);
                        } else {
                          ret.complete(t);
                        }
                      });
            } catch (Throwable closeEx) {
              if (ex != null) {
                ex.addSuppressed(closeEx);
                ret.completeExceptionally(ex);
              } else {
                ret.completeExceptionally(closeEx);
              }
            }
          });
      return ret;
    } catch (Throwable ex) {
      final CompletableFuture<T> ret = new CompletableFuture<>();
      try {
        resource
            .close()
            .whenComplete(
                (ig, closeEx) -> {
                  if (closeEx != null) {
                    ex.addSuppressed(closeEx);
                  }
                  ret.completeExceptionally(ex);
                });
      } catch (Throwable closeEx) {
        ex.addSuppressed(closeEx);
        ret.completeExceptionally(ex);
      }
      return ret;
    }
  }

  /**
   * Performs an action with an {@link AsyncCloseable} resource, ensuring that the resource is
   * {@link AsyncCloseable#close()} closed} after the action completes. The returned stage will
   * complete when the stage returned by the close has completed. Similar to a try-with-resources
   * block, the resource will be closed even if {@code actionUnderResources} throws an exception.
   *
   * <p>The returned stage will complete exceptionally in the following scenarios
   *
   * <ol>
   *   <li>{@code actionUnderResource} throws an exception
   *   <li>{@link AutoCloseable#close()} throws an exception
   *   <li>{@link AutoCloseable#close()} returns a stage that completes exceptionally
   * </ol>
   *
   * If the action produces an exception <b> and </b> {@link #close()} produces an exception the
   * exception produced by close will be added to the exception from {@code actionUnderResource} as
   * a suppressed exception.
   *
   * @param resource an {@link AsyncCloseable} which will be {{@link #close()} closed} after {@code
   *     actionUnderResource} is run
   * @param actionUnderResource an action to perform that uses {@code resource} to produce a result
   * @param <T> the type produced by {@code actionUnderResource}
   * @param <R> the {@link AsyncCloseable} resource type
   * @return a {@link CompletionStage} that completes with the result of {@code actionUnderResource}
   *     after close has completed or completes exceptionally
   */
  static <T, R extends AsyncCloseable> CompletionStage<T> tryWith(
      final R resource, final Function<? super R, ? extends T> actionUnderResource) {

    try {
      T t = actionUnderResource.apply(resource);
      try {
        return resource.close().thenApply(ig -> t);
      } catch (Throwable ex) {
        return FutureSupport.errorStage(ex);
      }
    } catch (Throwable ex) {
      try {
        return resource
            .close()
            .handle(
                (ig, closeEx) -> {
                  if (closeEx != null) {
                    ex.addSuppressed(closeEx);
                  }
                  throw ex;
                });
      } catch (Throwable closeEx) {
        ex.addSuppressed(closeEx);
        return FutureSupport.errorStage(ex);
      }
    }
  }
}
