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

package com.ibm.async_util;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BoundedAsyncChannelTest extends AbstractAsyncChannelTest {

  private BoundedAsyncChannel<Integer> channel;

  @Before
  public void makeChannel() {
    this.channel = AsyncChannels.bounded();
  }

  @Override
  boolean send(final Integer c) {
    return this.channel.send(c).toCompletableFuture().join();
  }

  @Override
  AsyncIterator<Integer> consumer() {
    return this.channel;
  }

  @Override
  void closeImpl() {
    this.channel.close();
  }

  @Test
  public void asyncCloseContractTest() {
    // accepted right away
    final CompletableFuture<Boolean> f1 = this.channel.send(1).toCompletableFuture();
    Assert.assertTrue(f1.isDone());
    Assert.assertTrue(f1.join());

    // waiting
    final CompletableFuture<Boolean> f2 = this.channel.send(2).toCompletableFuture();
    Assert.assertFalse(f2.isDone());

    // close
    final CompletableFuture<Void> closeFuture = this.channel.close().toCompletableFuture();

    Assert.assertFalse(f2.isDone());
    Assert.assertFalse(closeFuture.isDone());

    // send after close
    final CompletableFuture<Boolean> f3 = this.channel.send(3).toCompletableFuture();

    // consume a result
    Assert.assertEquals(1, this.channel.nextFuture().toCompletableFuture().join().left().get().intValue());

    // f2 should be done, and accepted
    Assert.assertTrue(f2.isDone());
    Assert.assertTrue(f2.join());

    Assert.assertEquals(2, this.channel.nextFuture().toCompletableFuture().join().left().get().intValue());
    Assert.assertFalse(this.channel.nextFuture().toCompletableFuture().join().isLeft());

    // close should be done, f3 should be done and rejected
    Assert.assertTrue(closeFuture.isDone());
    Assert.assertTrue(f3.isDone());
    Assert.assertFalse(f3.join());
  }

  @Override
  Optional<Integer> poll() {
    return this.channel.poll();
  }
}


