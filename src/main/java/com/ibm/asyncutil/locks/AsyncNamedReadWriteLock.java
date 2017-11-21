/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A mechanism used to acquire read-write locks shared by a common name. Acquisitions for a given
 * name will share exclusivity with other acquisitions of the same name, based on
 * {@link Object#equals(Object) object equality}. Acquisitions of different names may proceed
 * unobstructed.
 * <p>
 * Implementations will specify whether their lock acquisition is fair or not; this interface does
 * not define this requirement.
 * <p>
 * Note that implementations will generally employ an underlying {@link java.util.Map}; as such, the
 * same precautions must be taken regarding mutability of keys (names). Name objects should not
 * change from the time of acquisition to the time of release, with respect to their
 * {@link Object#equals(Object) equality} and {@link Object#hashCode() hash code} semantics. The
 * release methods of the returned {@link AsyncReadWriteLock.ReadLockToken} and
 * {@link AsyncReadWriteLock.WriteLockToken} may throw a
 * {@link java.util.ConcurrentModificationException} if such a modification is detected.
 *
 * @param <T> the type of named objects used to identify read-write locks
 */
public interface AsyncNamedReadWriteLock<T> {

  /**
   * Acquires the read lock associated with the given name. The returned stage will complete when
   * the lock is no longer exclusively held by a writer, and the read lock has been acquired. The
   * stage may already be complete if the write lock for the given name is not currently held.
   * <p>
   * The {@link AsyncReadWriteLock.ReadLockToken} held by the returned stage is used to release the
   * read lock after it has been acquired and the read-lock-protected action has completed.
   *
   * @param name to acquire read access for
   * @return A {@link CompletionStage} which will complete with a
   *         {@link AsyncReadWriteLock.ReadLockToken} when the read lock associated with
   *         {@code name} has been acquired
   */
  CompletionStage<AsyncReadWriteLock.ReadLockToken> acquireReadLock(T name);

  /**
   * Exclusively acquires the write lock associated with the given name. The returned stage will
   * complete when the lock is no longer held by any readers or another writer, and the write lock
   * has been exclusively acquired. The stage may already be complete if no locks for the given name
   * are currently held.
   * <p>
   * The {@link AsyncReadWriteLock.WriteLockToken} held by the returned stage is used to release the
   * write lock after it has been acquired and the write-lock-protected action has completed.
   *
   * @param name to acquire exclusive write access for
   * @return A {@link CompletionStage} which will complete with a
   *         {@link AsyncReadWriteLock.WriteLockToken} when the lock associated with {@code name}
   *         has been exclusively acquired
   */
  CompletionStage<AsyncReadWriteLock.WriteLockToken> acquireWriteLock(T name);

  /**
   * Attempts to acquire the read lock associated with the given name. If the associated write lock
   * is not currently held, the returned Optional will hold a ReadLockToken representing this
   * acquisition. Otherwise, the returned Optional will be empty.
   * <p>
   * The {@link AsyncReadWriteLock.ReadLockToken} held by the returned optional is used to release
   * the read lock after it has been acquired and the read-lock-protected action has completed.
   *
   * @param name to acquire read access for
   * @return An {@link Optional} holding a {@link AsyncReadWriteLock.ReadLockToken} if the write
   *         lock associated with {@code name} is not held; otherwise an empty Optional
   */
  Optional<AsyncReadWriteLock.ReadLockToken> tryReadLock(T name);

  /**
   * Attempts to acquire the write lock associated with the given name. If the associated write lock
   * or read lock is not currently held, the returned Optional will hold a WriteLockToken
   * representing this acquisition. Otherwise, the returned Optional will be empty.
   * <p>
   * The {@link AsyncReadWriteLock.WriteLockToken} held by the returned optional is used to release
   * the write lock after it has been acquired and the write-lock-protected action has completed.
   *
   * @param name to acquire exclusive write access for
   * @return An {@link Optional} holding a {@link AsyncReadWriteLock.WriteLockToken} if the lock
   *         associated with {@code name} is not held by a writer or any readers; otherwise an empty
   *         Optional
   */
  Optional<AsyncReadWriteLock.WriteLockToken> tryWriteLock(T name);

  /**
   * Creates an {@link AsyncNamedReadWriteLock}
   *
   * <p>
   * The returned lock is only guaranteed to meet the requirements of
   * {@link AsyncNamedReadWriteLock}; in particular, no guarantee of fairness is provided.
   *
   * @return a new {@link AsyncNamedReadWriteLock}
   */
  static <T> AsyncNamedReadWriteLock<T> create() {
    // fair for now, may be swapped with a more performant unfair version later
    return new FairAsyncNamedReadWriteLock<>();
  }

  /**
   * Creates a fair {@link AsyncNamedReadWriteLock}
   *
   * @return a new {@link AsyncNamedReadWriteLock} with a fair implementation
   */
  static <T> AsyncNamedReadWriteLock<T> createFair() {
    return new FairAsyncNamedReadWriteLock<>();
  }
}
