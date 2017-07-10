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
// Date: Feb 14, 2017
// ---------------------

package com.ibm.async_util.iteration;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.ibm.async_util.locks.AsyncSemaphore;
import com.ibm.async_util.locks.FairAsyncSemaphore;
import com.ibm.async_util.util.Either;

/**
 * Methods to construct various multi-producer-single-consumer (mpsc) AsyncChannels.
 *
 * @see AsyncChannel
 * @see BoundedAsyncChannel
 */
public final class AsyncChannels {
  private AsyncChannels() {}

  /**
   * Creates an unbounded AsyncChannel.
   *
   * <p>
   * Sends on an unbounded channel always complete synchronously, and throttling must be managed by
   * the senders to ensure senders don't get too far ahead of the consumer. {@link AsyncChannel} for
   * details.
   *
   * @return an {@link AsyncChannel}
   */
  public static <T> AsyncChannel<T> unbounded() {
    return new UnboundedChannel<>();
  }

  /**
   * Creates a bounded AsyncChannel.
   *
   * <p>
   * This channel can only accept one value at a time until it is consumed. It is useful when you
   * want to produce work potentially in parallel, but want to be throttled at the rate at which you
   * can consume this work. See {@link BoundedAsyncChannel} for details.
   *
   * @return a {@link BoundedAsyncChannel}
   */
  public static <T> BoundedAsyncChannel<T> bounded() {
    // this could be implemented more efficiently
    // 1. using an AsyncSemaphore(1) instead of a lock
    // 2. the AsyncSemaphore(1) actually ends up making this single consumer/single producer, so it
    // could potentially be done with a cheaper implementation
    return new BufferedChannel<>(1);
  }

  /**
   * Creates a buffered AsyncChannel.
   *
   * <p>
   * This channel can accept up to {@code maxBuffer} values before the futures returned by send
   * become delayed. See {@link BoundedAsyncChannel} for details
   *
   * @param maxBuffer the maximum number of values that the channel will accept before applying
   *        backpressure to senders
   * @param <T> the type of elements in the returned channel
   * @return a {@link BoundedAsyncChannel} with a buffer size of {@code maxBuffer} elements
   */
  public static <T> BoundedAsyncChannel<T> buffered(final int maxBuffer) {
    return new BufferedChannel<>(maxBuffer);
  }

  /**
   * A lock-free implementation of an unbounded {@link AsyncChannel}, which supports a
   * multi-producer single-consumer model. This implementation is Fair - if there are two
   * non-overlapping calls to send, the consumer will see the first call before the second.
   *
   * <p>
   * The approach is simple. There is a singly linked list of futures with a head and tail pointer,
   * with the following invariants
   * <li>There is always at least one node in the list
   *
   * <p>
   * While the Channel is open:
   * <li>After any call to send or nextFuture completes, there is exactly one uncompleted future in
   * the list
   * <li>After any call to send or nextFuture completes, the uncompleted future is pointed at by
   * head
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
  private static final class UnboundedChannel<T> implements AsyncChannel<T> {
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<UnboundedChannel, Node> TAIL_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(UnboundedChannel.class, Node.class, "tail");

    private static final Node<?> STOP = new Node<>(AsyncIterators.end());

    static {
      // link STOP to itself so that advancing does nothing when head reaches STOP
      STOP.next = UnboundedChannel.stopNode();
    }

    // head should only be accessed with exclusion by the consumer or by the completer of the
    // consumer's future
    private Node<T> head;

    @SuppressWarnings("restriction")
    @sun.misc.Contended
    private volatile Node<T> tail;

    private static class Node<T> extends CompletableFuture<Either<End, T>> {
      private Node<T> next = null;

      Node() {}

      Node(final Either<End, T> c) {
        super.complete(c);
      }
    }

    private UnboundedChannel() {
      final Node<T> headTail = new Node<>();
      this.head = headTail;
      // initially head = tail
      this.tail = headTail;
    }

    @Override
    public CompletableFuture<Either<End, T>> nextFuture() {
      // whenever we get a value from the head future, we should unlink that node, and move the head
      // pointer forward. We know head.next must exist, because a node's next value is always
      // updated before completion.
      return this.head.thenApply(res -> {
        // note: we don't need to check for exceptions here, because there is no way to send an
        // exceptional result into a channel
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
      sendImpl(AsyncIterators.end(), UnboundedChannel.stopNode());
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
   * that can be in the linked list in the backing {@link UnboundedChannel}.
   *
   * <p>
   * This implementation is Fair - if there are two non-overlapping calls to send, the consumer will
   * see the first call before the second.
   *
   * @param <T>
   */
  private static class BufferedChannel<T> implements BoundedAsyncChannel<T> {
    final AsyncSemaphore sendThrottle;
    final AsyncChannel<T> backingChannel;

    BufferedChannel(final int bufferSize) {
      this.sendThrottle = new FairAsyncSemaphore(bufferSize);
      this.backingChannel = AsyncChannels.unbounded();
    }

    @Override
    // ask for a future from the backing channel, release permit on completion since we
    // dequeued something.
    public CompletionStage<Either<End, T>> nextFuture() {
      return this.backingChannel
          .nextFuture()
          .thenApply(res -> {
            // only need to release if the backing channel is open. after it is closed, senders
            // will
            // release automatically
            res.forEach(ig -> {
            }, t -> this.sendThrottle.release());
            return res;
          });
    }

    @Override
    // acquire a permit and enqueue a node on the backing channel
    public CompletionStage<Boolean> send(final T item) {
      return this.sendThrottle
          // acquire a permit, this represents the node we put
          // in the queue in our underlying channel
          .acquire()
          .thenApply(ig -> {
            final boolean accepted = this.backingChannel.send(item);
            if (!accepted) {
              // the backing channel was closed, so our item will never be consumed. we should
              // release the permit we acquired
              this.sendThrottle.release();
            }
            return accepted;
          });
    }

    @Override
    public CompletionStage<Void> terminate() {
      // note we still want to respect the buffer here, fairness of our backing semaphore will
      // ensure that any sends queued before the terminate will still hit the backingChannel before
      // the
      // terminate does
      return this.sendThrottle
          .acquire()
          .thenApply(res -> {
            this.backingChannel.terminate();
            this.sendThrottle.release();
            return res;
          });
    }

    @Override
    public Optional<T> poll() {
      final Optional<T> poll = this.backingChannel.poll();
      // if we got a result, we should release a permit
      poll.ifPresent(ig -> this.sendThrottle.release());
      return poll;
    }
  }
}
