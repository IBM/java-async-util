/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

public class FairAsyncNamedReadWriteLockTest
    extends AbstractAsyncNamedReadWriteLockTest.AbstractAsyncNamedReadWriteLockFairnessTest {
  @Override
  protected <T> AsyncNamedReadWriteLock<T> getNamedReadWriteLock() {
    return new FairAsyncNamedReadWriteLock<T>();
  }

  @Override
  protected boolean isEmpty(final AsyncNamedReadWriteLock<?> anrwl) {
    return ((FairAsyncNamedReadWriteLock<?>) anrwl).isEmpty();
  }
}

