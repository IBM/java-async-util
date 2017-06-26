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
// @author: ivolvovski
//
// Date: Aug 9, 2015
// ---------------------

package com.ibm.async_util;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import com.ibm.async_util.AsyncLock.LockToken;


/**
 * A mechanism used to acquire mutual exclusion locks shared by a common name. Acquisitions for a
 * given name will share exclusivity with other acquisitions of the same name, based on
 * {@link Object#equals(Object) object equality}.
 * <p>
 * Implementations will specify whether their lock acquisition is fair or not; this interface does
 * not define this requirement.
 * <p>
 * Note that implementations will generally employ an underlying {@link java.util.Map}; as such, the
 * same precautions must be taken regarding mutability of keys (names). Name objects should not
 * change from the time of acquisition to the time of release, with respect to their
 * {@link Object#equals(Object) equality} and {@link Object#hashCode() hash code} semantics. The
 * release methods of the returned {@link LockToken} may throw a
 * {@link java.util.ConcurrentModificationException} if such a modification is detected.
 * 
 * @param <T> the type of named objects used to identify locks
 */
public interface AsyncNamedLock<T> {

  /**
   * Acquire the lock associated with the given name. If the associated lock is not currently held,
   * the returned future will be immediately complete. Otherwise, the returned future will complete
   * when the lock is no longer held by other acquisitions.
   * <p>
   * The {@link LockToken} held by the returned future is used to release the lock after it has been
   * acquired and the lock-protected action has completed.
   */
  public CompletionStage<LockToken> acquireLock(final T name);

  /**
   * Attempt to acquire the lock associated with the given name. If the associated lock is not
   * currently held, the returned future will be immediately complete. Otherwise, the returned
   * future will complete when the lock is no longer held by other acquisitions.
   * <p>
   * The {@link LockToken} held by the returned Optional is used to release the lock after it has
   * been acquired and the lock-protected action has completed.
   */
  public Optional<LockToken> tryLock(final T name);
}
