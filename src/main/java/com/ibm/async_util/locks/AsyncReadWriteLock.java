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
// Date: Apr 6, 2015
// ---------------------

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
  public CompletionStage<ReadLockToken> acquireReadLock();

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
  public Optional<ReadLockToken> tryReadLock();


  /**
   * Exclusively acquire this write-lock. If another associated write-lock or read-lock has not been
   * acquired, the returned future will be immediately complete. Otherwise, the returned future will
   * complete when the lock is no longer held by any readers or an exclusive writer.
   * <p>
   * The {@link WriteLockToken} held by the returned future is used to release the write lock after
   * it has been acquired and the write-lock-protected action has completed.
   */
  public CompletionStage<WriteLockToken> acquireWriteLock();

  /**
   * Attempt to immediately acquire the write lock, returning a populated {@link Optional} if the
   * lock is not currently held by a writer or any readers.
   * 
   * @return An {@link Optional} holding a {@link WriteLockToken} if the lock is not held by a
   *         writer or any readers; otherwise an empty Optional
   */
  public Optional<WriteLockToken> tryWriteLock();

  /**
   * A lock token indicating that the associated lock has been acquired for reader access. Once the
   * protected action is completed, the lock may be released by calling
   * {@link ReadLockToken#releaseReadLock()}
   */
  public interface ReadLockToken {
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
  public interface WriteLockToken {
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
}