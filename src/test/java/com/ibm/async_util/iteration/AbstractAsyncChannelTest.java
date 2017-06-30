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
// Date: Feb 16, 2017
// ---------------------

package com.ibm.async_util.iteration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractAsyncChannelTest {
  private final static int NUM_THREADS = 5;
  private final static int ITEMS_PER_THREAD = 2000;
  private final static int NUM_ITEMS = (NUM_THREADS * ITEMS_PER_THREAD);

  private ThreadPoolExecutor consumerThread;
  private AtomicBoolean closed;

  private final List<Integer> validationQueue = new ArrayList<>();
  private final ConcurrentLinkedQueue<Integer> queue =
      new ConcurrentLinkedQueue<>();

  @Before
  public void before() {
    this.closed = new AtomicBoolean(false);
    this.consumerThread = new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    final List<Integer> randomItems = new ArrayList<>();
    for (int i = 0; i < NUM_ITEMS; i++) {
      randomItems.add((int) (Math.random() * NUM_ITEMS));
    }
    setupQueue(randomItems);
  }
  

  private void setupQueue(final List<Integer> queue) {
    this.validationQueue.clear();
    this.queue.clear();
    for (int i = 0; i < NUM_ITEMS; i++) {
      this.validationQueue.add(queue.get(i));
    }
    this.queue.addAll(this.validationQueue.stream().collect(Collectors.toList()));
  }

  @After
  public void after() throws InterruptedException {
    this.consumerThread.shutdown();
    Assert.assertTrue(this.consumerThread.awaitTermination(1, TimeUnit.SECONDS));
  }

  private void close() {
    closeImpl();
    this.closed.set(true);
  }

  abstract boolean send(final Integer c);

  abstract void closeImpl();

  abstract AsyncIterator<Integer> consumer();

  abstract Optional<Integer> poll();

  @Test
  public void singleProducerPollTest() {
    singleProducer(this::pollConsumer);
  }

  @Test
  public void singleProducerTest() {
    singleProducer(this::normalConsumer);
  }

  private void singleProducer(
      final Supplier<CompletionStage<List<Integer>>> consumer) {
    final CompletionStage<List<Integer>> future = consumer.get();
    CompletableFuture<Boolean> prodFuture = CompletableFuture.supplyAsync(() -> {
      Integer next;
      while ((next = this.queue.poll()) != null) {
        Assert.assertTrue(send(next));
      }
      close();

      // should be rejected
      Assert.assertFalse(send(1));
      return true;
    });
    // check if producer threw an exception
    prodFuture.join();
    final List<Integer> lis = future.toCompletableFuture().join();
    Assert.assertTrue(this.queue.isEmpty());
    Assert.assertEquals(this.validationQueue.size(), lis.size());
    Assert.assertEquals(this.validationQueue, lis);
  }

  private List<Queue<Integer>> chunk() {
    final List<Queue<Integer>> chunks =
        IntStream.range(0, NUM_THREADS).boxed()
            .map(ig -> new ArrayDeque<Integer>())
            .collect(Collectors.toList());
    for (int i = 0; i < NUM_ITEMS; i++) {
      chunks.get(i % NUM_THREADS).add(this.queue.poll());
    }
    return chunks;
  }


  @Test
  public void multiProducerTest() throws InterruptedException {
    multiProducer(this::normalConsumer);
  }

  @Test
  public void multiProducerPollTest() throws InterruptedException {
    multiProducer(this::pollConsumer);
  }

  private void multiProducer(
      final Supplier<CompletionStage<List<Integer>>> consumer)
      throws InterruptedException {
    final CompletionStage<List<Integer>> future = consumer.get();
    final List<Queue<Integer>> chunks = chunk();
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      final Queue<Integer> chunk = chunks.get(i);
      futures.add(CompletableFuture.runAsync(() -> {
        Integer next;
        while ((next = chunk.poll()) != null) {
          Assert.assertTrue(send(next));
        }
      }));
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    // all senders finished, end the stream
    close();

    // should be rejected
    Assert.assertFalse(send(1));

    final List<Integer> lis = future.toCompletableFuture().join();
    Assert.assertTrue(this.queue.isEmpty());
    Assert.assertEquals(this.validationQueue.size(), lis.size());
    Assert.assertEquals(new HashSet<>(this.validationQueue), new HashSet<>(lis));
  }

  @Test
  public void multiProducerFairnessTest() throws InterruptedException, ExecutionException, TimeoutException {
    multiProducerFairness(this::normalConsumer);
  }

  @Test
  public void multiProducerFairnessPollTest() throws InterruptedException, ExecutionException, TimeoutException {
    multiProducerFairness(this::pollConsumer);
  }

  private void multiProducerFairness(
      final Supplier<CompletionStage<List<Integer>>> consumer)
      throws InterruptedException, ExecutionException, TimeoutException {
    // add num_threads of each number
    final List<Integer> ordered = IntStream.range(0, ITEMS_PER_THREAD).boxed()
        .flatMap(i -> Stream
            .generate(() -> i)
            .limit(NUM_THREADS))
        .collect(Collectors.toList());

    final CompletionStage<List<Integer>> future = consumer.get();

    for (int i = 0; i < ITEMS_PER_THREAD; i++) {
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      for (int j = 0; j < NUM_THREADS; j++) {
        final int finali = i;
        // add the number i for each thread
        futures.add(CompletableFuture.runAsync(() -> {
          Assert.assertTrue(send(finali));
        }));
      }
      // wait for all threads
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);
    }

    // all senders finished, end the stream
    close();

    // should be rejected
    Assert.assertFalse(send(1));

    final List<Integer> lis = future.toCompletableFuture().join();
    Assert.assertEquals(ordered.size(), lis.size());

    // order should be preserved
    Assert.assertEquals(ordered, lis);
  }

  @Test
  public void multiProducerSendAfterStopTest() {
    multiProducerSendAfterStop(this::normalConsumer);
  }

  @Test
  public void multiProducerSendAfterStopPollTest() {
    multiProducerSendAfterStop(this::pollConsumer);
  }

  private void multiProducerSendAfterStop(
      final Supplier<CompletionStage<List<Integer>>> consumer) {
    final AtomicLong sentBeforeKill = new AtomicLong();
    final AtomicBoolean killed = new AtomicBoolean();
    final int killAt = NUM_ITEMS / 2;

    final CompletionStage<List<Integer>> future = consumer.get();
    final List<Queue<Integer>> chunks = chunk();
    List<CompletableFuture<Void>> producers = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      final Queue<Integer> chunk = chunks.get(i);
      producers.add(CompletableFuture.runAsync(() -> {
        Integer next;
        while ((next = chunk.poll()) != null) {
          final long sendNum = sentBeforeKill.getAndIncrement();
          if (sendNum == killAt) {
            // close
            close();
            killed.set(true);
          } else if (killed.get()) {
            // send a value that indicates it was sent after kill
            Assert.assertFalse(send(-1));
          } else {
            // we could have raced with killed and been killed since - send is not guaranteed to
            // succeed
            send(next);
          }
        }
      }));
    }
    CompletableFuture.allOf(producers.toArray(new CompletableFuture[0])).join();
    final List<Integer> lis = future.toCompletableFuture().join();
    Assert.assertFalse(lis.contains(-1));

  }

  @Test
  public void pollAfterCloseEmptyTest() {
    send(1);
    Assert.assertEquals(poll().get().intValue(), 1);
    close();
    Assert.assertFalse(poll().isPresent());
    Assert.assertFalse(poll().isPresent());
  }

  @Test
  public void pollAfterCloseRemainingTest() {
    send(1);
    Assert.assertEquals(poll().get().intValue(), 1);
    send(2);
    close();
    Assert.assertEquals(poll().get().intValue(), 2);
    Assert.assertFalse(poll().isPresent());
    Assert.assertFalse(poll().isPresent());
  }

  private CompletionStage<List<Integer>> pollConsumer() {
    final CompletableFuture<List<Integer>> sf = new CompletableFuture<>();
    this.consumerThread.submit(() -> {
      final List<Integer> lis = new ArrayList<>();
      while (!this.closed.get()) {
        poll().ifPresent(lis::add);
        Thread.yield();
      }
      // consume whatever's left after close
      Integer i;
      while ((i = poll().orElse(null)) != null) {
        lis.add(i);
      }
      sf.complete(lis);
    });
    return sf;
  }

  private CompletionStage<List<Integer>> normalConsumer() {
    return consumer().collect(Collectors.toList());
  }
}


