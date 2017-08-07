/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.util;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * An object that may hold resources that must be explicitly released, where the release may be
 * performed asynchronously.
 *
 * <p>
 * Examples of such resources are manually managed memory, open file handles, socket descriptors
 * etc. While similar to {@link AutoCloseable}, this interface should be used when the resource
 * release operation may possibly be async. For example, if an object is thread-safe and has many
 * consumers, an implementation may require all current ongoing operations to complete before
 * resources are relinquished. A common way to implement this pattern for a thread-safe object with
 * asynchronous methods is by using an {@link com.ibm.async_util.locks.ObservableEpoch}.
 *
 * <p>
 * May be used with the methods {@link StageSupport#tryWith(AsyncCloseable, Function)},
 * {@link StageSupport#tryComposeWith(AsyncCloseable, Function)} to emulate the behavior of a try
 * with resources block.
 */
@FunctionalInterface
public interface AsyncCloseable {
  /**
   * Relinquishes any resources associated with this object.
   *
   * @return a {@link CompletionStage} that completes when all resources associated with this object
   *         have been released, or with an exception if the resources cannot be released.
   */
  CompletionStage<Void> close();

}
