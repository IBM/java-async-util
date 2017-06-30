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
// @author: rkhadiwala
//
// Date: Feb 17, 2017
// ---------------------

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
    this.channel.close();
  }

  @Override
  Optional<Integer> poll() {
    return this.channel.poll();
  }

}


