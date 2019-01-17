/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.cleversafe.util.async.AbstractAsyncReadWriteLockTest.AbstractAsyncReadWriteLockReaderPriorityTest;

/**
 * A unit test of {@link ReaderPriorityAsyncReadWriteLock} which executes the tests in
 * {@link AbstractAsyncReadWriteLockReaderPriorityTest} because the primary test class can't inherit from two test
 * bases.
 * 
 * TODO(junit5) refactor the Lock test hierarchy to use interfaces with default methods for fairness offshoots and
 * extend several interfaces within a single test class
 */
public class ReaderPriorityAsyncNamedReadWriteLockTest2 extends AbstractAsyncReadWriteLockReaderPriorityTest {
  @Override
  protected AsyncReadWriteLock getReadWriteLock() {
    return new AsyncNamedRWLockAsRWLock(new ReaderPriorityAsyncNamedReadWriteLock<Object>());
  }
}

