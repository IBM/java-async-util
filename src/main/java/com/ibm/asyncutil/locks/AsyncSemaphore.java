/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.concurrent.CompletionStage;

/**
 * An asynchronously acquirable counting semaphore
 * <p>
 * Implementations will specify whether their permit acquisition and release is fair or not; this
 * interface does not define this requirement.
 */
public interface AsyncSemaphore {

  /**
   * Acquires the given number of permits from the semaphore, returning a stage which will complete
   * when all of the permits are exclusively acquired.
   * <p>
   * If the permits are available immediately, the returned stage may be already complete (but need
   * not be).
   * <p>
   * If the permits are not available immediately, the acquisition will enter a queue and an
   * incomplete stage will be returned. Semantics of the waiter queue, including ordering policies,
   * are implementation specific and will be defined by the given implementing class. The returned
   * stage will complete when sufficient permits have been {@link #release(long) released} and
   * assigned to this acquisition by the governing queue policy.
   *
   * @param permits A non-negative number of permits to acquire from the semaphore
   * @return a {@link CompletionStage} which will be completed when all {@code permits} have been acquired
   * @throws IllegalArgumentException if the requested permits are negative, or exceed any
   *         restrictions enforced by the given implementation
   */
  CompletionStage<Void> acquire(long permits);

  /**
   * Releases the given number of permits to the semaphore.
   * <p>
   * If there are unfulfilled acquires pending, this method will release permits to the waiting
   * acquisitions based on the implementation's release and acquire policies. Consequently, this
   * method may complete a number of waiting stages and execute the corresponding observers.
   *
   * @param permits A non-negative number of permits to release to the semaphore
   * @throws IllegalArgumentException if the released permits are negative, or exceed any
   *         restrictions enforced by the given implementation
   */
  void release(long permits);

  /**
   * Attempts to acquire the given number of permits from the semaphore, returning a boolean
   * indicating whether all of the permits were immediately available and have been exclusively
   * acquired.
   * <p>
   * Implementations may define precise behavior of this method with respect to competing
   * acquisitions, e.g. whether permits may be acquired while other acquisitions are waiting. This
   * interface only requires that this method will succeed when the given permits are available and
   * there are no other acquisitions queued.
   *
   * @param permits A non-negative number of permits to acquire from the semaphore
   * @return true iff all of the requested permits are available, and have been immediately acquired
   * @throws IllegalArgumentException if the requested permits are negative, or exceed any
   *         restrictions enforced by the given implementation
   */
  boolean tryAcquire(long permits);

  /**
   * Acquires all permits that are immediately available.
   * <p>
   * After this call -- provided there are no intermediate {@link #release(long) releases} -- any
   * attempt to {@link #acquire(long) acquire} will queue and any {@link #tryAcquire(long)
   * tryAcquire} will fail.
   *
   * @return the number of permits that were available and have been drained
   */
  long drainPermits();

  /**
   * Gets the number of currently available permits.
   * <p>
   * The bounds of the returned value are not defined; an implementation may, for example, choose to
   * represent waiting acquisitions as holding negative permits, and thus the value returned by this
   * method could be negative. Furthermore, a positive number of permits returned by this method may
   * not indicate that such permits are acquirable, as the waiter-queue policy may prohibit
   * fulfilling further acquisitions.
   * <p>
   * This value is produced on a best-effort basis, and should not be used for any control logic.
   * Generally it is only useful in testing, debugging, or statistics purposes.
   *
   * @return the number of currently available permits
   */
  long getAvailablePermits();

  /**
   * Gets the number of unfulfilled acquisitions waiting on this semaphore's permits.
   * <p>
   * This value is produced on a best-effort basis, and should not be used for any control logic.
   * Generally it is only useful in testing, debugging, or statistics purposes.
   *
   * @return the number of unfulfilled acquisitions waiting on this semaphore's permits
   */
  int getQueueLength();

  /**
   * Acquires 1 permit from the semaphore as if by calling {@link #acquire(long)} with an argument
   * of 1.
   *
   * @return a {@link CompletionStage} which will complete when 1 permit has been successfully acquired
   * @see #acquire(long)
   */
  default CompletionStage<Void> acquire() {
    return acquire(1L);
  }

  /**
   * Releases 1 permit from the semaphore as if by calling {@link #release(long)} with an argument
   * of 1.
   *
   * @see #release(long)
   */
  default void release() {
    release(1L);
  }

  /**
   * Attempts to acquire 1 permit from the semaphore as if by calling {@link #tryAcquire(long)} with
   * an argument of 1.
   *
   * @return true iff the single request permit is available, and has been immediately acquired
   * @see #tryAcquire(long)
   */
  default boolean tryAcquire() {
    return tryAcquire(1L);
  }

}
