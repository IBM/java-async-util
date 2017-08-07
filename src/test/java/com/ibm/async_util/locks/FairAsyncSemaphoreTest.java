/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import org.junit.Test;

public class FairAsyncSemaphoreTest
    extends AbstractAsyncSemaphoreTest.AbstractAsyncSemaphoreFairnessTest {
  public FairAsyncSemaphoreTest() {
    super(FairAsyncSemaphore.MAX_PERMITS);
  }

  @Override
  protected AsyncSemaphore createSemaphore(final long initialPermits) {
    return new FairAsyncSemaphore(initialPermits);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceededMinConstructor() {
    createSemaphore(Long.MIN_VALUE);
  }
}
