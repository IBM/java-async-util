/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

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


