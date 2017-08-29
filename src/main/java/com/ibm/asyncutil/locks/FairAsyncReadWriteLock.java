/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * An implementation of the {@link AsyncReadWriteLock} interface which enforces reader/writer
 * fairness -- i.e. new readers will queue behind waiting writers
 */
public class FairAsyncReadWriteLock implements AsyncReadWriteLock {
  /*
   * Locks are maintained as a linked-queue of epoch nodes (see ObservableEpoch for documentation).
   * The current epoch, at the head of the list, allows readers to enter and exit until a write lock
   * is acquired, which terminates the epoch. Using the epoch mechanism ensures the following
   * properties: 1) exactly 1 writer can acquire a node for writing, sending other writers to the
   * queue 2) new readers can be rejected by nodes after the write lock acquisition, sending them to
   * the next queued node 3) the acquiring writer has a means of being signaled when all readers
   * before him have released.
   *
   * When a writer acquires a write lock, first it terminates the epoch which will reject subsequent
   * readers and writers. The terminating writer must then push a new epoch node to the head of the
   * queue, and link as its predecessor, for subsequent locks to enter. The terminating writer
   * maintains an implicit read lock on its successor node in order to block future writers until
   * release. When a writer releases, it triggers the read future of its successor node, allowing
   * the readers waiting on that future to proceed, and also releasing its implicit read lock in
   * order to permit write acquisitions. When a reader releases, it exits its epoch node which may
   * trigger that node's write future (depending on the state of the epoch, see documentation under
   * whenCleared)
   */

  volatile Node head;

  public FairAsyncReadWriteLock() {
    // no (real) predecessor, no implicit reader, init 0 count
    final Node h = new Node(0);
    // readers may proceed immediately if the lock hasn't been acquired
    h.readFuture.complete(h);
    this.head = h;
  }

  /**
   * If the epoch has not been terminated, it's available for readers to acquire and wait on the
   * read future. The read future is triggered by the preceding writer's release.
   *
   * Writer acquisition terminates the epoch, forcing subsequent readers to re-read the head. The
   * writer future is triggered on the epoch's clearing i.e. when all readers have exited (including
   * the preceding writer's implicit read lock)
   */
  @SuppressWarnings("serial")
  static class Node extends AbstractSimpleEpoch
      implements ReadLockToken, AsyncStampedLock.Stamp {

    class WriteLockFutureToken extends CompletableFuture<WriteLockToken>
        implements WriteLockToken {

      @Override
      public void releaseLock() {
        downgradeLock().releaseLock();
      }

      @Override
      public ReadLockToken downgradeLock() {
        final Node n = Node.this.next;
        if (!n.readFuture.complete(n)) {
          throw new IllegalStateException("released or downgraded write lock not in locked state");
        }

        // because writers keep an implicit read lock on the successor, downgrading is trivial
        return n;
      }

      private void complete() {
        if (!complete(this)) {
          throw new AssertionError();
        }
      }
    }

    final CompletableFuture<ReadLockToken> readFuture;
    final WriteLockFutureToken writeLockToken;
    Node next;

    // state used in stack unrolling
    Node prev;
    private Thread releaseThread;

    /**
     * Constructor for queue sentinel nodes. The unrolling mechanism requires every node to have a
     * modifiable predecessor; this constructor provides such a predecessor for the first real node
     * in the lock queue
     */
    private Node() {
      this.readFuture = null;
      this.writeLockToken = null;
    }

    /**
     * Constructor for the first node in the lock queue (no real predecessor, internally employs a
     * sentinel predecessor)
     */
    Node(final int initialEntrants) {
      this(initialEntrants, new Node());
      this.prev.next = this;
    }

    /**
     * Constructor for nodes <i>not</i> first in the lock queue (known predecessor)
     */
    Node(final int initialEntrants, final Node predecessor) {
      lazySet(initialEntrants);
      this.prev = predecessor;
      this.readFuture = new CompletableFuture<>();
      this.writeLockToken = new WriteLockFutureToken();
    }

    @Override
    public final void releaseLock() {
      // reader finished -> exit the node's epoch
      close();
    }

    @Override
    final void onCleared() {
      /*
       * When all the readers have left a terminated node, the writer may proceed; specifically by
       * completing the write future. Unbounded recursion needs to be prevented, however, so
       * completing the future isn't so simple.
       *
       * To prevent stack growth in recursion we use stack unrolling. The principle mechanism is
       * this: before a release triggers the potential recursion (completing the write future), the
       * triggering node will record its current thread; if nested calls find that their thread is
       * the same as their predecessor (the triggering node) then they can unroll and be released in
       * the triggering method. The nested call (a queued node) informs the triggering node of what
       * to unroll by unlinking its predecessor from the queue, taking the place of the triggering
       * node. The triggering method finds that its predecessor's 'next' has been re-linked (i.e.
       * replaced in the queue) so it begins a new trigger on that new node.
       *
       * You may think it's strange that none of this unrolling code uses conventional thread-safety
       * mechanisms -- access to prev could span threads! That's true, but it's correct: there are 2
       * fields of `this.prev` that are potentially accessed externally, `releaseThread` and `next`.
       *
       * - Non-null values of releaseThread are only relevant if it's equal to Thread.current(), so
       * if we read a value that's out of date, we don't care unless it's from the current thread --
       * but the current thread can't be out of date with itself!
       *
       * - prev's next is only modified under the condition that prev's releaseThread equals
       * Thread.current() -- so the predecessor is on the same thread, there's no thread-safety
       * issue
       */
      this.releaseThread = Thread.currentThread();
      if (this.releaseThread.equals(this.prev.releaseThread)) {
        /*
         * because the threads match, we know our predecessor is in the process of completing its
         * write future. Because 'this' node has arrived at onCleared as well, we need to complete
         * our own future, but not in this stack frame. Instead, relink ourselves in the position
         * that our predecessor currently has, which will mark us as the next node to trigger
         */
        final Node unlink = this.prev;
        this.prev = unlink.prev;
        unlink.prev.next = this;
        unlink.prev = unlink.next = null;
        unlink.releaseThread = null;
      } else {
        /*
         * The sentinel always points to the node whose write future needs to be completed.
         * Initially it points to 'this' but recursive calls can unlink this node to point sentinel
         * at a new node for completion.
         */
        final Node sentinel = this.prev;
        Node toComplete;
        do {
          toComplete = sentinel.next;
          sentinel.next = null;

          toComplete.writeLockToken.complete();

        } while (sentinel.next != null);

        // done unrolling. prevent any ABA thread matches by clearing the release thread
        toComplete.releaseThread = null;
        toComplete.prev = null; // assist GC by clearing predecessor pointers
      }
    }

    final boolean tryReadLock() {
      // the read future must be done to indicate no locked predecessors
      // and entry must succeed to indicate no write locks
      return this.readFuture.isDone() && internalEnter() == Status.NORMAL;
    }

    final boolean tryWriteLock() {
      // there must be no readers, which includes implicit predecessors, and no writers, together
      // designated as zero; CAS failure indicates read or write acquire by someone else.
      return compareAndSet(0, TERMINATED);
    }

    @Override
    public final boolean validate() {
      /*
       * a load fence is required here because the operations within the optimistic read don't
       * necessarily create any dependency with the volatile read of the lock's state. Without such
       * a dependency, the JMM permits ordering reads during that optimistic period *after* this
       * call to validate, making the query essentially meaningless. Using a loadfence forces any
       * loads during the optimistic period to occur before this method call, and thus before the
       * lock's state check
       */
      if (!PlatformDependent.loadFence()) {
        // fence ordering is required, simply fail if it's not supported
        return false;
      }

      /*
       * once issued, a stamp will be valid until the writer on that node is allowed to proceed,
       * i.e. the write future completes. isCleared and writeFuture.isDone are equivalent (clearing
       * triggers future completion), but the former saves us a memory indirection
       */
      return !isCleared();
    }
  }

  @Override
  public final CompletionStage<ReadLockToken> acquireReadLock() {
    Node h;
    while ((h = this.head).internalEnter() != AbstractSimpleEpoch.Status.NORMAL) {
      // writer has locked this node. re-read head until we have a non-locked node
    }
    return h.readFuture;
  }

  /**
   * Attempt to immediately acquire the read lock, returning a populated {@link Optional} if the
   * lock is not currently held by a writer and there are no writers queued in waiting (i.e. this
   * acquire is "fair" and will not barge ahead of waiting writers).
   */
  @Override
  public final Optional<ReadLockToken> tryReadLock() {
    final Node h = this.head;
    return h.tryReadLock() ? Optional.of(h) : Optional.empty();
  }

  @Override
  public final CompletionStage<WriteLockToken> acquireWriteLock() {
    Node h;
    while ((h = this.head).internalTerminate() != AbstractSimpleEpoch.Status.NORMAL) {
      // some other writer has locked this node. re-read head until we have a non-locked node
    }
    /*
     * we've successfully write locked a node, so we must push a new node to head that will block
     * subsequent access. We must also track this new node as our successor in order to trigger
     * their readers/writers when we release. Lastly, the node must start with count 1 so that we
     * block its potential write acquisition by being an implicit reader
     */
    this.head = h.next = new Node(1, h);
    return h.writeLockToken;
  }

  @Override
  public final Optional<WriteLockToken> tryWriteLock() {
    final Node h = this.head;
    if (h.tryWriteLock()) {
      this.head = h.next = new Node(1, h);
      return Optional.of(h.writeLockToken);
    } else {
      return Optional.empty();
    }
  }
}
