/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * An asynchronously acquirable read-write lock.
 * <p>
 * Implementations will specify whether their lock acquisition is fair or not; this interface does
 * not define this requirement.
 */
public interface AsyncReadWriteLock {

  /**
   * Acquire this read-lock. If the associated write-lock has not been acquired, the returned future
   * will be immediately complete. Otherwise, the returned future will complete when the lock is no
   * longer exclusively acquired by a writer.
   * <p>
   * The {@link ReadLockToken} held by the returned future is used to release the read lock after it
   * has been acquired and the read-lock-protected action has completed.
   */
  CompletionStage<ReadLockToken> acquireReadLock();

  /**
   * Attempt to immediately acquire the read lock, returning a populated {@link Optional} if the
   * lock is not currently held by a writer.
   * <p>
   * Implementations may define whether this attempt can succeed while a writer is waiting to
   * acquire ("barging"). This interface only requires that the attempt will succeed if all writers
   * have released and no new writers are acquiring.
   *
   * @return An {@link Optional} holding a {@link ReadLockToken} if the lock is not held by a
   *         writer; otherwise an empty Optional
   */
  Optional<ReadLockToken> tryReadLock();


  /**
   * Exclusively acquire this write-lock. If another associated write-lock or read-lock has not been
   * acquired, the returned future will be immediately complete. Otherwise, the returned future will
   * complete when the lock is no longer held by any readers or an exclusive writer.
   * <p>
   * The {@link WriteLockToken} held by the returned future is used to release the write lock after
   * it has been acquired and the write-lock-protected action has completed.
   */
  CompletionStage<WriteLockToken> acquireWriteLock();

  /**
   * Attempt to immediately acquire the write lock, returning a populated {@link Optional} if the
   * lock is not currently held by a writer or any readers.
   *
   * @return An {@link Optional} holding a {@link WriteLockToken} if the lock is not held by a
   *         writer or any readers; otherwise an empty Optional
   */
  Optional<WriteLockToken> tryWriteLock();

  /**
   * A lock token indicating that the associated lock has been acquired for reader access. Once the
   * protected action is completed, the lock may be released by calling
   * {@link ReadLockToken#releaseReadLock()}
   */
  interface ReadLockToken {
    /**
     * Release this read lock, possibly allowing writers to enter once all read locks have been
     * released.
     */
    void releaseReadLock();
  }

  /**
   * A lock token indicating that the associated lock has been exclusively acquired for writer
   * access. Once the protected action is completed, the lock may be released by calling
   * {@link WriteLockToken#releaseWriteLock()}
   */
  interface WriteLockToken {
    /**
     * Release this write lock, allowing readers or other writers to acquire it.
     */
    void releaseWriteLock();

    /**
     * Downgrade this write lock acquisition to a read lock acquisition without any intermediate
     * release. This may allow other waiting readers to proceed with their acquisitions. Other
     * writers, however, may not proceed until the returned read lock token (and any others that
     * become acquire) is released.
     * <p>
     * The returned {@link ReadLockToken} becomes the principle token for this acquisition; this
     * calling WriteLockToken should not be released afterwards, and may be abandoned freely.
     *
     * @return a ReadLockToken representing read lock exclusivity on the lock
     */
    ReadLockToken downgradeLock();
  }

  /**
   * Creates an {@link AsyncReadWriteLock}
   *
   * <p>
   * The returned lock is only guaranteed to meet the requirements of {@link AsyncReadWriteLock}; in
   * particular, no guarantee of fairness is provided.
   *
   * @return a new {@link AsyncReadWriteLock}
   */
  static AsyncReadWriteLock create() {
    // fair for now, may be swapped with a more performant unfair version later
    return new FairAsyncReadWriteLock();
  }

  /**
   * Creates a fair {@link AsyncReadWriteLock}
   *
   * @return a new {@link AsyncReadWriteLock} with a fair implementation
   */
  static AsyncReadWriteLock createFair() {
    return new FairAsyncReadWriteLock();
  }
}
