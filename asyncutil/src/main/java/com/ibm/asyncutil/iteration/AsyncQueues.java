/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.iteration;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.ibm.asyncutil.locks.AsyncSemaphore;
import com.ibm.asyncutil.locks.FairAsyncSemaphore;
import com.ibm.asyncutil.util.Either;

/**
 * Methods to construct various multi-producer-single-consumer (mpsc) AsyncQueues.
 *
 * @see AsyncQueue
 * @see BoundedAsyncQueue
 */
public final class AsyncQueues {
  private AsyncQueues() {}

  /**
   * Creates an unbounded AsyncQueue.
   *
   * <p>
   * Sends on an unbounded queue always complete synchronously, and throttling must be managed by
   * the senders to ensure senders don't get too far ahead of the consumer. See {@link AsyncQueue}
   * for details.
   *
   * @return an {@link AsyncQueue}
   * @see AsyncQueue
   */
  public static <T> AsyncQueue<T> unbounded() {
    return new UnboundedQueue<>();
  }

  /**
   * Creates a bounded AsyncQueue.
   *
   * <p>
   * This queue can only accept one value at a time until it is consumed. This may be useful when
   * you want to produce work potentially in parallel, but want to be throttled at the rate at which
   * you can consume this work. See {@link BoundedAsyncQueue} for details.
   *
   * @return a {@link BoundedAsyncQueue}
   */
  public static <T> BoundedAsyncQueue<T> bounded() {
    // this could be implemented more efficiently
    // 1. using an AsyncSemaphore(1) instead of a lock
    // 2. the AsyncSemaphore(1) actually ends up making this single consumer/single producer, so it
    // could potentially be done with a cheaper implementation
    return new BufferedQueue<>(1);
  }

  /**
   * Creates a buffered AsyncQueue.
   *
   * <p>
   * This queue can accept up to {@code maxBuffer} values before the futures returned by send become
   * delayed. See {@link BoundedAsyncQueue} for details
   *
   * @param maxBuffer the maximum number of values that the queue will accept before applying
   *        backpressure to senders
   * @param <T> the type of elements in the returned queue
   * @return a {@link BoundedAsyncQueue} with a buffer size of {@code maxBuffer} elements
   */
  public static <T> BoundedAsyncQueue<T> buffered(final int maxBuffer) {
    return new BufferedQueue<>(maxBuffer);
  }

  /**
   * A lock-free implementation of an unbounded {@link AsyncQueue}, which supports a multi-producer
   * single-consumer model. This implementation is Fair - if there are two non-overlapping calls to
   * send, the consumer will see the first call before the second.
   *
   * <p>
   * The approach is simple. There is a singly linked list of futures with a head and tail pointer,
   * with the following invariants
   * <li>There is always at least one node in the list
   *
   * <p>
   * While the Queue is open:
   * <li>After any call to send or nextStage completes, there is exactly one uncompleted future in
   * the list
   * <li>After any call to send or nextStage completes, the uncompleted future is pointed at by head
   *
   * <p>
   * The list starts with an initial uncompleted future. When send is called, the sender attempts to
   * update the tail with a new uncompleted future via CAS. When it succeeds, it completes the
   * former tails future. Readers just return the future pointed at by head, when that future
   * completes the head pointer is moved forward.
   *
   * <p>
   * Concretely, if there are values to read when the reader comes in, it simply observes an already
   * completed future. If there are no values to read, it will wait on the head future (which is
   * also the tail in this case), which will be completed by whichever sender updates the tail.
   *
   * <p>
   * When a sender sends an Optional.empty, the tail is replaced with STOP marker. Future senders
   * see the tail and don't try to update it. Once the reader hits the empty tail, it stops moving
   * the head
   *
   * @param <T>
   */
  private static final class UnboundedQueue<T> implements AsyncQueue<T> {
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<UnboundedQueue, Node> TAIL_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(UnboundedQueue.class, Node.class, "tail");

    private static final Node<?> STOP = new Node<>(End.end());

    static {
      // link STOP to itself so that advancing does nothing when head reaches STOP
      STOP.next = UnboundedQueue.stopNode();
    }

    // head should only be accessed with exclusion by the consumer or by the completer of the
    // consumer's future
    private Node<T> head;

    private volatile Node<T> tail;

    private static class Node<T> extends CompletableFuture<Either<End, T>> {
      private Node<T> next = null;

      Node() {}

      Node(final Either<End, T> c) {
        super.complete(c);
      }
    }

    private UnboundedQueue() {
      final Node<T> headTail = new Node<>();
      this.head = headTail;
      // initially head = tail
      this.tail = headTail;
    }

    @Override
    public CompletableFuture<Either<End, T>> nextStage() {
      // whenever we get a value from the head future, we should unlink that node, and move the head
      // pointer forward. We know head.next must exist, because a node's next value is always
      // updated before completion.
      return this.head.thenApply(res -> {
        // note: we don't need to check for exceptions here, because there is no way to send an
        // exceptional result into a queue
        this.head = this.head.next;
        return res;
      });
    }

    @Override
    public Optional<T> poll() {
      // head can never complete exceptionally so this should never throw
      final Either<End, T> currentResult = this.head.getNow(null);
      if (currentResult != null) {
        // we're going to consume a value, move the header pointer forward
        this.head = this.head.next;
        return currentResult.right();
      }

      // future wasn't completed
      return Optional.empty();
    }

    @Override
    public boolean send(final T item) {
      return sendImpl(Either.right(item), new Node<>());
    }

    @Override
    public void terminate() {
      sendImpl(End.end(), UnboundedQueue.stopNode());
    }

    private boolean sendImpl(final Either<End, T> item, final Node<T> newTail) {
      Node<T> oldTail;
      do {
        oldTail = this.tail;
        // if at any point we see that the oldTail is the stop marker, we can just bail
        if (oldTail == STOP) {
          return false;
        }
      } while (!TAIL_UPDATER.compareAndSet(this, oldTail, newTail));

      // we swapped the old tail. it is ours to modify and complete
      // set next
      oldTail.next = newTail;
      // if oldTail == head, this will notify the waiter, and oldTail will be unlinked. Otherwise,
      // we're just adding a completed future to the list
      oldTail.complete(item);
      return true;
    }

    @SuppressWarnings("unchecked")
    private static <T> Node<T> stopNode() {
      return (Node<T>) STOP;
    }
  }

  /**
   * Implementation is backed by an {@link AsyncSemaphore} which throttles the number of elements
   * that can be in the linked list in the backing {@link UnboundedQueue}.
   *
   * <p>
   * This implementation is Fair - if there are two non-overlapping calls to send, the consumer will
   * see the first call before the second.
   *
   * @param <T>
   */
  private static class BufferedQueue<T> implements BoundedAsyncQueue<T> {
    final AsyncSemaphore sendThrottle;
    final AsyncQueue<T> backingQueue;

    BufferedQueue(final int bufferSize) {
      this.sendThrottle = new FairAsyncSemaphore(bufferSize);
      this.backingQueue = AsyncQueues.unbounded();
    }

    @Override
    // ask for a future from the backing queue, release permit on completion since we
    // dequeued something.
    public CompletionStage<Either<End, T>> nextStage() {
      return this.backingQueue
          .nextStage()
          .thenApply(res -> {
            // only need to release if the backing queue is open. after it is closed, senders
            // will
            // release automatically
            res.forEach(ig -> {
            }, t -> this.sendThrottle.release());
            return res;
          });
    }

    @Override
    // acquire a permit and enqueue a node on the backing queue
    public CompletionStage<Boolean> send(final T item) {
      return this.sendThrottle
          // acquire a permit, this represents the node we put
          // in the queue in our underlying queue
          .acquire()
          .thenApply(ig -> {
            final boolean accepted = this.backingQueue.send(item);
            if (!accepted) {
              // the backing queue was closed, so our item will never be consumed. we should
              // release the permit we acquired
              this.sendThrottle.release();
            }
            return accepted;
          });
    }

    @Override
    public CompletionStage<Void> terminate() {
      // note we still want to respect the buffer here, fairness of our backing semaphore will
      // ensure that any sends queued before the terminate will still hit the backingQueue before
      // the
      // terminate does
      return this.sendThrottle
          .acquire()
          .thenApply(res -> {
            this.backingQueue.terminate();
            this.sendThrottle.release();
            return res;
          });
    }

    @Override
    public Optional<T> poll() {
      final Optional<T> poll = this.backingQueue.poll();
      // if we got a result, we should release a permit
      poll.ifPresent(ig -> this.sendThrottle.release());
      return poll;
    }
  }
}
