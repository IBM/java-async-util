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
// @author: rkhadiwala
//
// Date: Jul 9, 2016
// ---------------------

package com.ibm.async_util;

/**
 * sometimes you need a modifiable final variable...
 * 
 * @param <T>
 */
public class Reference<T> {
  private T t;

  public Reference(final T t) {
    this.t = t;
  }

  public T get() {
    return this.t;
  }

  public void set(final T t) {
    this.t = t;
  }

  /**
   * Set the reference to the given value, returning the previous value of the reference
   */
  public T getAndSet(final T t) {
    final T get = this.t;
    this.t = t;
    return get;
  }

  /**
   * Set the reference to the given value, returning this new value
   */
  public T setAndGet(final T t) {
    this.t = t;
    return t;
  }

}


