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
// Date: Nov 10, 2016
// ---------------------

package com.ibm.async_util.locks;

/**
 * An asynchronously acquirable read-write lock which additionally provides an optimistic read mode.
 * Optimistic read mode can be thought of as a weak reader mode which does not prevent writers from
 * acquiring the lock.
 *
 * <p>In practice, optimistic reads of brief read-only sections can reduce memory contention. Great
 * care must be taken when reading mutable variables, however, as they can change at any point and
 * knowledge of the underlying accessed structures is necessary to ensure a consistent view of the
 * variables.
 *
 * <p>Consider the following example. The BitSet class is not inherently thread-safe, so we need a
 * lock to protect it. Further, our hypothetical use case exhibits only rare modifications but
 * extremely frequent queries, which may justify an optimistic read design:
 *
 * <pre>{@code
 * BitSet bits = new BitSet();
 *
 * Future<Boolean> isSet(int idx) {
 *   // first attempt to optimistically read; if it fails,
 *   // fall back to full read lock
 *   final Stamp stamp = this.lock.tryOptimisticRead();
 *
 *   // lock may already be held, first check the stamp
 *   if (stamp != null) {
 *
 *     // BitSet is internally "safe" to concurrent modification during reads
 *     // i.e. structures cannot be corrupted, but results may be inconsistent
 *     // for example these individual bits might be separately modified
 *     final boolean optimistic = this.bits.get(idx) && this.bits.get(idx+1);
 *
 *     // the read value can only be considered correct if
 *     // the write lock was not acquired in the interim
 *     if (stamp.validate()) {
 *       return Futures.of(optimistic);
 *     }
 *   }
 *
 *   // otherwise, if the write lock was already held, or acquired afterwards,
 *   // acquire the full read lock for a consistent result (fall back from optimism)
 *   return this.lock.acquireReadLock().map(lockToken -> {
 *     try {
 *       return this.bits.get(idx) && this.bits.get(idx+1);
 *     } finally {
 *       lockToken.releaseReadLock();
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>This interface draws inspiration from the standard library's {@link
 * java.util.concurrent.locks.StampedLock} but certain implementation details may differ. Identical
 * behavior should not be expected from both locking facilities beyond what is explicitly
 * documented.
 *
 * <p>Implementations will specify whether their lock acquisition is fair or not; this interface
 * does not define this requirement.
 *
 * @see AsyncReadWriteLock
 * @see java.util.concurrent.locks.StampedLock
 */
public interface AsyncStampedLock extends AsyncReadWriteLock {

  /**
   * Attempt to acquire a {@link Stamp} in optimistic-read mode if the lock is not already
   * write-locked. The stamp may subsequently be {@link Stamp#validate() validated} to check whether
   * the write lock has been acquired
   *
   * @return a non-null Stamp if the lock is not currently write-locked. Otherwise, returns {@code
   *     null}
   */
  // API note: why a nullable instead of an Optional<Stamp> like the similar try*Lock methods?
  // a few reasons:
  //
  // - the common use case of tryOptimistic -> check null -> validate -> fall back to readlock is
  // cumbersome with Optional chaining compared to if-blocks
  // - tryOptimisticRead is fundamentally intended for high-performance areas; an additional
  // object wrapper is undesirable
  Stamp tryOptimisticRead();

  /**
   * An object indicating a successful optimistic read attempt.
   *
   * @see AsyncStampedLock#tryOptimisticRead()
   */
  interface Stamp {

    /**
     * Check whether the associated lock's write mode has been acquired in the time after this stamp
     * was issued.
     *
     * @return true iff the stamp is still valid i.e. write lock has not been acquired since this
     *     stamp was issued
     */
    boolean validate();
  }

  /**
   * Creates an {@link AsyncStampedLock}
   *
   * <p>The returned lock is only guaranteed to meet the requirements of {@link AsyncStampedLock}; in
   * particular, no guarantee of fairness is provided.
   *
   * @return a new {@link AsyncStampedLock}
   */
  static AsyncStampedLock create() {
    // fair for now, may be swapped with a more performant unfair version later
    return new FairAsyncStampedLock();
  }

  /**
   * Creates a fair {@link AsyncStampedLock}
   *
   * @return a new {@link AsyncStampedLock} with a fair implementation
   */
  static AsyncStampedLock createFair() {
    return new FairAsyncStampedLock();
  }
}
