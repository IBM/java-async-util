/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


/**
 * An {@link AsyncLock} which enforces fairness in its acquisition ordering
 */
public class FairAsyncLock implements AsyncLock {
  /*
   * Each attempt to acquire the lock is assigned a unique node in the backing FIFO ordered queue.
   * The head node is always unclaimed; when a participant attempts to acquire the lock, it installs
   * a new node to the head and claims the node that was just replaced by this process. Each node
   * holds a future which is triggered when the preceding node's `releaseLock` method has been
   * called (excluding the initial node, whose future is complete during construction of the lock).
   *
   * The current holder of the lock -- the assignee of the node with a complete future and not yet
   * called `releaseLock` -- holds the implicit tail of the queue. When release is called on this
   * node, the node unlinks itself from the queue and triggers its successor's future, allowing the
   * next waiter to proceed
   */

  private static final Node NULL_PREDECESSOR = new Node();
  private static final AtomicReferenceFieldUpdater<FairAsyncLock, Node> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FairAsyncLock.class, Node.class, "head");
  private volatile Node head;

  public FairAsyncLock() {
    final Node n = new Node();
    n.prev = NULL_PREDECESSOR;

    // the first lock is immediately available
    n.complete(n);

    this.head = n;
  }

  private static final class Node extends CompletableFuture<LockToken>
      implements LockToken {
    Node next;
    Node prev;
    private Thread releaseThread;

    @Override
    public void releaseLock() {
      // see FairAsyncReadWriteLock for an explanation of the unrolling code
      if (this.next == null) {
        throw new IllegalStateException("released lock not in locked state");
      }

      this.releaseThread = Thread.currentThread();
      if (this.releaseThread.equals(this.prev.releaseThread)) {
        // unlink nested node from queue
        this.prev.next = this.next;
        this.next.prev = this.prev;
        this.next = null; // break chain
      } else {
        do {
          final Node n = this.next;
          this.next = null;
          n.complete(n);
          // recursive calls will set our next to non-null
        } while (this.next != null);
      }

      this.releaseThread = null;
      this.prev = null; // break chains for gc purposes
    }

  }

  @Override
  public CompletionStage<LockToken> acquireLock() {
    final Node newHead = new Node();
    final Node oldHead = UPDATER.getAndSet(this, newHead);

    newHead.prev = oldHead;
    oldHead.next = newHead;
    return oldHead;
  }

  // Node is AutoCloseable, but here is not acquired so should not be released
  @SuppressWarnings("resource")
  @Override
  public Optional<LockToken> tryLock() {
    final Node oldHead = this.head;
    final Node newHead;
    // if the head is unlocked (future is done)
    // and no one else locks (the CAS succeeds)
    if (oldHead.isDone() && UPDATER.compareAndSet(this, oldHead, newHead = new Node())) {
      newHead.prev = oldHead;
      oldHead.next = newHead;
      return Optional.of(oldHead);
    } else {
      return Optional.empty();
    }
  }
}
