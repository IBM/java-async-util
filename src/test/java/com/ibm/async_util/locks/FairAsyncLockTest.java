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
// Date: Dec 28, 2016
// ---------------------

package com.ibm.async_util.locks;

public class FairAsyncLockTest extends AbstractAsyncLockTest.AbstractAsyncLockFairnessTest {
  @Override
  protected AsyncLock getLock() {
    return new FairAsyncLock();
  }
}
