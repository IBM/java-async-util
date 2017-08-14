/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * An asynchronously acquirable mutual exclusion lock.
 *
 * <p>
 * Implementations will specify whether their lock acquisition is fair or not; this interface does
 * not define this requirement.
 */
public interface AsyncLock {

  /**
   * Exclusively acquire this lock. If the lock is not currently held, the returned future will be
   * immediately complete. Otherwise, the returned future will complete when the lock is exclusively
   * acquired by this caller.
   *
   * <p>
   * The {@link LockToken} held by the returned future is used to release the lock after it has been
   * acquired and the lock-protected action has completed.
   */
  CompletionStage<LockToken> acquireLock();

  /**
   * Attempt to immediately acquire the lock, returning a populated {@link Optional} if the lock is
   * not currently held.
   *
   * @return An {@link Optional} holding a {@link LockToken} if the lock is not held; otherwise an
   *         empty Optional
   */
  Optional<LockToken> tryLock();

  /**
   * A lock token indicating that the associated lock has been exclusively acquired. Once the
   * protected action is completed, the lock may be released by calling
   * {@link LockToken#releaseLock()}
   */
  interface LockToken extends AutoCloseable {
    /** Release this lock, allowing others to acquire it. */
    void releaseLock();

    /**
     * Release this lock, allowing others to acquire it.
     *
     * <p>
     * {@inheritDoc}
     */
    @Override
    default void close() {
      releaseLock();
    }
  }

  /**
   * Creates an {@link AsyncLock}
   *
   * <p>
   * The returned lock is only guaranteed to meet the requirements of {@link AsyncLock}; in
   * particular, no guarantee of fairness is provided.
   *
   * @return a new {@link AsyncLock}
   */
  static AsyncLock create() {
    // fair for now, may be swapped with a more performant unfair version later
    return new FairAsyncLock();
  }

  /**
   * Creates a fair {@link AsyncLock}
   *
   * @return a new {@link AsyncLock} with a fair implementation
   */
  static AsyncLock createFair() {
    return new FairAsyncLock();
  }
}
