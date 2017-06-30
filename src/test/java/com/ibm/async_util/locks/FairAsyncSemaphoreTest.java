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
// Date: Aug 8, 2016
// ---------------------

package com.ibm.async_util.locks;

import org.junit.Test;

public class FairAsyncSemaphoreTest extends AbstractAsyncSemaphoreTest.AbstractAsyncSemaphoreFairnessTest {
  public FairAsyncSemaphoreTest() {
    super(FairAsyncSemaphore.MAX_PERMITS);
  }

  @Override
  protected AsyncSemaphore createSemaphore(final long initialPermits) {
    return new FairAsyncSemaphore(initialPermits);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceededMaxConstructor() {
    createSemaphore(Long.MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceededMinConstructor() {
    createSemaphore(Long.MIN_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceededMaxAcquire() {
    createSemaphore(0).acquire(Long.MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceededMaxRelease() {
    createSemaphore(0).release(Long.MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceededMaxTryAcquire() {
    createSemaphore(0).acquire(Long.MAX_VALUE);
  }
}
