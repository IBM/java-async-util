/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * An implementation of the {@link AsyncNamedLock} interface which enforces fair ordering in lock
 * acquisition.
 * <p>
 * {@code null} values are not permitted for use as names.
 */
public class FairAsyncNamedLock<T> implements AsyncNamedLock<T> {
  private final FairAsyncNamedReadWriteLock<T> fanrwl = new FairAsyncNamedReadWriteLock<>();

  @Override
  public CompletionStage<AsyncLock.LockToken> acquireLock(final T name) {
    return this.fanrwl.acquireWriteLock(name).thenApply(writeToken -> writeToken::releaseWriteLock);
  }

  @Override
  public Optional<AsyncLock.LockToken> tryLock(final T name) {
    return this.fanrwl.tryWriteLock(name).map(writeToken -> writeToken::releaseWriteLock);
  }

}
