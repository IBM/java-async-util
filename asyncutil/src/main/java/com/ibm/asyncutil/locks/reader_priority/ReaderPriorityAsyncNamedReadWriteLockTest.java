/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.cleversafe.util.async.AbstractAsyncNamedReadWriteLockTest.AbstractAsyncNamedReadWriteLockReaderPriorityTest;

public class ReaderPriorityAsyncNamedReadWriteLockTest extends AbstractAsyncNamedReadWriteLockReaderPriorityTest {

  @Override
  protected <T> AsyncNamedReadWriteLock<T> getNamedReadWriteLock() {
    return new ReaderPriorityAsyncNamedReadWriteLock<T>();
  }

  @Override
  protected boolean isEmpty(final AsyncNamedReadWriteLock<?> narwls) {
    return ((ReaderPriorityAsyncNamedReadWriteLock<?>) narwls).isEmpty();
  }
}
