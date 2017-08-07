/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.iteration;

import java.util.Optional;

import org.junit.Before;

public class AsyncChannelTest extends AbstractAsyncChannelTest {

  private AsyncChannel<Integer> channel;

  @Before
  public void makeChannel() {
    this.channel = AsyncChannels.unbounded();
  }

  @Override
  boolean send(final Integer c) {
    return this.channel.send(c);
  }

  @Override
  AsyncIterator<Integer> consumer() {
    return this.channel;
  }

  @Override
  void closeImpl() {
    this.channel.terminate();
  }

  @Override
  Optional<Integer> poll() {
    return this.channel.poll();
  }

}


