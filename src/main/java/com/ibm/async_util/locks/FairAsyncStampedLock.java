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
// Date: Nov 14, 2016
// ---------------------

package com.ibm.async_util.locks;

/**
 * An {@link AsyncStampedLock} implementation which provides fair ordering of acquisitions, like
 * that of {@link FairAsyncReadWriteLock}. The {@link #tryOptimisticRead()} method also employs fair
 * ordering, in that a stamp will not be issued if there are queued writers waiting to acquire the
 * lock. {@link Stamp#validate() Validation} of an already issued stamp, however, does not abide by
 * this ordering; it will only fail validation once a writer has successfully acquired the lock
 * (irrespective of queued writers)
 */
public class FairAsyncStampedLock extends FairAsyncReadWriteLock implements AsyncStampedLock {
  /*
   * The precise details of the optimistic read ordering for this implementation result from the
   * behavior of the underlying FairAsyncReadWriteLock implementation. Once a writer enters the
   * queue, the previously existing state is no longer accessible (in a thread-safe manner) and
   * without the previous state it's not possible to say whether readers are permitted to run.
   * Circumventing this limitation may be possible, but would likely involve changes to the
   * underlying lock structure, at which point an independent implementation would likely be more
   * practical than piggy-backing the existing rwlock.
   *
   * Such a hypothetical improvement would allow optimistic reads to proceed even with queued
   * writers, which would be favorable particularly when full read-locks are held for a relatively
   * long time, possibly creating many queued writers.
   *
   * As it stands, this implementation is fairly effective when writer acquisition is rare, which in
   * general is a cause for looking into optimistic read strategies to begin with.
   */

  @Override
  public Stamp tryOptimisticRead() {
    final Node h = this.head;
    /*
     * in order to issue a stamp, the node must be in read-mode i.e. the read future must be done,
     * and the write future must not be done. h.isCleared is equivalent to h.writeFuture.isDone
     * without the indirection
     */
    return (!h.isCleared()) && h.readFuture.isDone() ? h : null;
  }
}
