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
// Date: Jan 12, 2017
// ---------------------

package com.ibm.async_util;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import com.ibm.async_util.AsyncLock.LockToken;

/**
 * An implementation of the {@link AsyncNamedLock} interface which enforces fair ordering in lock
 * acquisition.
 * <p>
 * {@code null} values are not permitted for use as names.
 */
public class FairAsyncNamedLock<T> implements AsyncNamedLock<T> {
  private final FairAsyncNamedReadWriteLock<T> fanrwl = new FairAsyncNamedReadWriteLock<>();

  @Override
  public CompletionStage<LockToken> acquireLock(final T name) {
    return this.fanrwl.acquireWriteLock(name).thenApply(writeToken -> writeToken::releaseWriteLock);
  }

  @Override
  public Optional<LockToken> tryLock(final T name) {
    return this.fanrwl.tryWriteLock(name).map(writeToken -> writeToken::releaseWriteLock);
  }

}
