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

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * An object that may hold resources that must be explicitly released, where the release may be
 * performed asynchronously.
 *
 * <p>Examples of such resources are manually managed memory, open file handles, socket descriptors
 * etc. While similar to {@link AutoCloseable}, this interface should be used when the resource
 * release operation may possibly be async. For example, if an object is thread-safe and has many
 * consumers, an implementation may require allOf current ongoing operations to complete before
 * resources are relinquished. A common way to implement this pattern for a thread-safe object with
 * asynchronous methods is by using an {@link com.ibm.async_util.locks.ObservableEpoch}.
 *
 * <p>May be used with the methods {@link FutureSupport#tryWithStagedClose(CompletionStage,
 * Function)}, {@link FutureSupport#tryComposeWithStagedClose(CompletionStage, Function)} to emulate
 * the behavior of a try with resources block.
 */
@FunctionalInterface
public interface AsyncCloseable {
  /**
   * Relinquishes any resources associated with this object.
   *
   * @return a {@link CompletionStage} that completes when any resources associated with this object
   *     have been released, or with an exception if the resources cannot be released.
   */
  CompletionStage<Void> close();
}
