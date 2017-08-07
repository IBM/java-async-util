/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A mechanism used to acquire mutual exclusion locks shared by a common name. Acquisitions for a
 * given name will share exclusivity with other acquisitions of the same name, based on
 * {@link Object#equals(Object) object equality}.
 *
 * <p>
 * Implementations will specify whether their lock acquisition is fair or not; this interface does
 * not define this requirement.
 *
 * <p>
 * Note that implementations will generally employ an underlying {@link java.util.Map}; as such, the
 * same precautions must be taken regarding mutability of keys (names). Name objects should not
 * change from the time of acquisition to the time of release, with respect to their
 * {@link Object#equals(Object) equality} and {@link Object#hashCode() hash code} semantics. The
 * release methods of the returned {@link AsyncLock.LockToken} may throw a
 * {@link java.util.ConcurrentModificationException} if such a modification is detected.
 *
 * @param <T> the type of named objects used to identify locks
 */
public interface AsyncNamedLock<T> {

  /**
   * Acquire the lock associated with the given name. If the associated lock is not currently held,
   * the returned future will be immediately complete. Otherwise, the returned future will complete
   * when the lock is no longer held by other acquisitions.
   *
   * <p>
   * The {@link AsyncLock.LockToken} held by the returned future is used to release the lock after
   * it has been acquired and the lock-protected action has completed.
   */
  CompletionStage<AsyncLock.LockToken> acquireLock(final T name);

  /**
   * Attempt to acquire the lock associated with the given name. If the associated lock is not
   * currently held, the returned future will be immediately complete. Otherwise, the returned
   * future will complete when the lock is no longer held by other acquisitions.
   *
   * <p>
   * The {@link AsyncLock.LockToken} held by the returned Optional is used to release the lock after
   * it has been acquired and the lock-protected action has completed.
   */
  Optional<AsyncLock.LockToken> tryLock(final T name);

  /**
   * Creates an {@link AsyncNamedLock}
   *
   * <p>
   * The returned lock is only guaranteed to meet the requirements of {@link AsyncNamedLock}; in
   * particular, no guarantee of fairness is provided.
   *
   * @return a new {@link AsyncNamedLock}
   */
  static <T> AsyncNamedLock<T> create() {
    // fair for now, may be swapped with a more performant unfair version later
    return new FairAsyncNamedLock<>();
  }

  /**
   * Creates a fair {@link AsyncNamedLock}
   *
   * @return a new {@link AsyncNamedLock} with a fair implementation
   */
  static <T> AsyncNamedLock<T> createFair() {
    return new FairAsyncNamedLock<>();
  }
}
