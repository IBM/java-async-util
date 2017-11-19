/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.iteration;

import java.util.Optional;

import org.junit.Before;

public class AsyncQueueTest extends AbstractAsyncQueueTest {

  private AsyncQueue<Integer> queue;

  @Before
  public void makeQueue() {
    this.queue = AsyncQueues.unbounded();
  }

  @Override
  boolean send(final Integer c) {
    return this.queue.send(c);
  }

  @Override
  AsyncIterator<Integer> consumer() {
    return this.queue;
  }

  @Override
  void closeImpl() {
    this.queue.terminate();
  }

  @Override
  Optional<Integer> poll() {
    return this.queue.poll();
  }

}


