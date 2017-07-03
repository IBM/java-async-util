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

import com.ibm.async_util.iteration.AsyncIterator.End;
import com.ibm.async_util.util.Combinators;
import com.ibm.async_util.util.Either;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BufferedAsyncChannelTest extends AbstractAsyncChannelTest {
  private final static int BUFFER = 5;
  private BoundedAsyncChannel<Integer> channel;

  @Before
  public void makeChannel() {
    this.channel = AsyncChannels.buffered(BUFFER);
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
    this.channel.terminate();
  }


  @Test
  public void bufferedTest() {
    // first five futures should be done immediately
    IntStream.range(0, BUFFER)
        .mapToObj(i -> this.channel.send(i).toCompletableFuture())
        .forEach(f -> {
          Assert.assertTrue(f.isDone());
          Assert.assertTrue(f.join());
        });

    // next 5 should all wait
    final List<CompletableFuture<Boolean>> collect = IntStream.range(0, BUFFER)
        .mapToObj(this.channel::send)
        .map(CompletionStage::toCompletableFuture)
        .map(f -> {
          Assert.assertFalse(f.isDone());
          return f;
        })
        .collect(Collectors.toList());

    for (int i = 0; i < BUFFER; i++) {
      CompletableFuture<Either<End, Integer>> fut = this.channel.nextFuture().toCompletableFuture();

      // could change with impl, but with a full channel, futures should already be completed
      Assert.assertTrue(fut.isDone());
      // not closed
      Assert.assertTrue(fut.join().isRight());

      // impl supports fairness (for now), every release, the next waiting future should complete
      for (int j = 0; j < BUFFER; j++) {
        Assert.assertTrue(collect.get(j).isDone() == (j <= i));
      }
    }

    this.channel.terminate();
    for (int i = 0; i < BUFFER * 5; i++) {
      if (i % 2 == 0) {
        this.channel.terminate();
      } else {
        this.channel.send(1);
      }
    }
    this.channel.consume().toCompletableFuture().join();
  }

  @Test
  public void asyncCloseContractTest() {
    // accepted right away
    final List<CompletableFuture<Boolean>> immediate = IntStream
        .range(0, BUFFER)
        .mapToObj(this.channel::send)
        .map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());

    Assert.assertTrue(Combinators.allOf(immediate).toCompletableFuture().join().stream().allMatch(b -> b));

    final List<CompletableFuture<Boolean>> delayeds = IntStream
        .range(0, BUFFER)
        .mapToObj(i -> this.channel.send(i + BUFFER))
        .map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());
    Assert.assertFalse(delayeds.stream().map(CompletableFuture::isDone).anyMatch(b -> b));

    // terminate
    final CompletableFuture<Void> closeFuture = this.channel.terminate().toCompletableFuture();

    Assert.assertFalse(delayeds.stream().map(Future::isDone).anyMatch(b -> b));
    Assert.assertFalse(closeFuture.isDone());

    // send after terminate
    final CompletableFuture<Boolean> rejected = this.channel.send(3).toCompletableFuture();

    for (int i = 0; i < BUFFER; i++) {
      // consume one item
      Assert.assertEquals(i, this.channel.nextFuture().toCompletableFuture().join().right().get().intValue());
      // delayeds less than item should be done
      Assert.assertTrue(delayeds.stream().limit(i + 1).map(Future::isDone).allMatch(b -> b));
      Assert
          .assertTrue(delayeds.stream().limit(i + 1).map(CompletableFuture::join).allMatch(b -> b));
      // delayeds more than item should be pending
      Assert.assertFalse(delayeds.stream().skip(i + 1).map(Future::isDone).anyMatch(b -> b));
      // terminate should not be done until all delayeds are done

      // according to the contract, the terminate future could be done when the last delayed is
      // accepted, however it is not required. only check that if there is outstanding acceptable
      // work, we don't finish terminate
      if (i == BUFFER - 1) {
        Assert.assertFalse(closeFuture.isDone());
      }
    }

    // consume delayed results
    for (int i = BUFFER; i < 2 * BUFFER; i++) {
      Assert.assertEquals(i, this.channel.nextFuture().toCompletableFuture().join().right().get().intValue());
    }
    Assert.assertFalse(this.channel.nextFuture().toCompletableFuture().join().isRight());
    Assert.assertTrue(closeFuture.isDone());
    Assert.assertTrue(rejected.isDone());
    Assert.assertFalse(rejected.join());
  }

  @Override
  Optional<Integer> poll() {
    return this.channel.poll();
  }

}


