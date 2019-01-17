/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.cleversafe.util.async.AbstractAsyncReadWriteLockTest.AbstractAsyncReadWriteLockReaderPriorityTest;

public class ReaderPriorityAsyncReadWriteLockTest
    extends AbstractAsyncReadWriteLockReaderPriorityTest {

  @Override
  protected AsyncReadWriteLock getReadWriteLock() {
    return new ReaderPriorityAsyncReadWriteLock();
  }
}
