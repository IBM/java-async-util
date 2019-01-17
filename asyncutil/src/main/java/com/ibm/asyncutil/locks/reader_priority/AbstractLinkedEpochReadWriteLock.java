/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import java.util.Optional;

import org.cleversafe.util.PlatformDependent;

/**
 * This abstract class forms the basis of AsyncReadWriteLock implementations based on a linked queue of
 * {@link AbstractSimpleEpoch} nodes, where readers are entrants to the epoch and writers are terminators.
 * <p>
 * Concrete implementations may use different epoch behavior to manifest different fairness behavior.
 */
abstract class AbstractLinkedEpochReadWriteLock implements AsyncReadWriteLock {
  /*
   * Locks are maintained as a linked-queue of epoch nodes (see ObservableEpoch for documentation). The current epoch,
   * at the head of the list, allows readers to enter and exit until a write lock is acquired, which terminates the
   * epoch. Using the epoch mechanism ensures the following properties: 1) exactly 1 writer can acquire a node for
   * writing, sending other writers to the queue 2) new readers can be rejected by nodes after the write lock
   * acquisition, sending them to the next queued node 3) the acquiring writer has a means of being signaled when all
   * readers before him have released.
   * 
   * When a writer acquires a write lock, first it terminates the epoch which will reject subsequent readers and
   * writers. The terminating writer must then push a new epoch node to the head of the queue, and link as its
   * predecessor, for subsequent locks to enter. The terminating writer maintains an implicit read lock on its successor
   * node in order to block future writers until release. When a writer releases, it triggers the read future of its
   * successor node, allowing the readers waiting on that future to proceed, and also releasing its implicit read lock
   * in order to permit write acquisitions. When a reader releases, it exits its epoch node which may trigger that
   * node's write future (depending on the state of the epoch, see documentation under onCleared)
   * 
   * By virtue of property (2), the default epoch mechanism creates a fair ordering between readers and writers, because
   * a new reader cannot enter a node which a writer has already claimed. The epoch behavior can be adjusted to favor
   * readers by permitting new entrants after termination: so long as the entrant count is non-zero (even on terminated
   * nodes) new readers may be allowed to join, but the writer must be released at any point when all entrants have left
   * -- that last point resolves races around releasing the epoch and triggering futures. This allows reader
   * favoritism/barging/unfairness for an individual node. One additional change is necessary to make the lock itself
   * unfair, which is to introduce an interior node pointer to the oldest node that readers may still join. This
   * interior node would follow the most recent write-lock release, where new readers would be allowed to barge ahead of
   * queued writers, as opposed to the head pointer which follows the most recent write-lock acquisition. The
   * scaffolding to support unfairness is implemented here, while the concrete details (the interior pointer and unfair
   * epochs) are left to concrete implementors.
   */

  /**
   * Nodes which are still accepting readers will increment their counter and return the read future for read
   * acquisitions. The read future is triggered by the preceding writer's release.
   * 
   * Writer acquisition terminates the epoch, (possibly) forcing subsequent readers to re-read the lock's node pointer.
   * The writer future is triggered on the epoch's clearing i.e. when all readers have exited (including the preceding
   * writer's implicit read lock)
   */
  static abstract class Node extends AbstractSimpleEpoch
      implements ReadLockToken, AsyncStampedLock.Stamp {

    class WriteLockPromiseToken extends Promise<WriteLockToken>
        implements WriteLockToken {
      @Override
      public final void releaseWriteLock() {
        downgradeLock().releaseReadLock();
      }

      @Override
      public final ReadLockToken downgradeLock() {
        final Node n = Node.this.next;
        if (!n.readFuture.tryComplete(n)) {
          throw new IllegalStateException("released or downgraded write lock not in locked state");
        }

        // because writers keep an implicit read lock on the successor, downgrading is trivial
        return n;
      }

      /**
       * Trigger this write lock promise, allowing the blocked waiter (if any) to proceed
       */
      private void complete() {
        if (!tryComplete(this)) {
          throw new AssertionError();
        }
      }
    }

    final Promise<ReadLockToken> readFuture;
    final WriteLockPromiseToken writeLockPromiseToken;
    Node next;

    // state used in stack unrolling
    Node prev;
    private Thread releaseThread;

    /**
     * Constructor for queue sentinel nodes. The unrolling mechanism requires every node to have a modifiable
     * predecessor; this constructor provides such a predecessor for the first real node in the lock queue
     */
    private Node() {
      this.readFuture = null;
      this.writeLockPromiseToken = null;
    }

    /**
     * Constructor for nodes <i>not</i> first in the lock queue (known predecessor)
     */
    Node(final int initialEntrants) {
      // use a weak set because nodes are always published safely
      super(initialEntrants, true);
      this.readFuture = new Promise<>();
      this.writeLockPromiseToken = new WriteLockPromiseToken();
    }

    /**
     * Link the given node mutually with this one, setting this.next to node, and node.prev to this.
     * 
     * @return the given argument node
     */
    Node linkNext(final Node node) {
      this.next = node;
      node.prev = this;
      return node;
    }

    /**
     * Invoked when a writer has acquired this node exclusively. This will be called either before the future is
     * triggered (for {@link AbstractLinkedEpochReadWriteLock#acquireWriteLock() asynchronous acquisitions}) or before
     * the acquisition method returns (for {@link AbstractLinkedEpochReadWriteLock#tryWriteLock() synchronous
     * acquisitions})
     * <p>
     * By default, this method does nothing. Implementors may override it, for example, to advance the interior reader
     * node in reader-priority implementations.
     */
    void writerAcquired() {
      // default does nothing
    }

    @Override
    public final void releaseReadLock() {
      // reader finished -> exit the node's epoch
      close();
    }

    @Override
    final void onCleared() {
      writerAcquired();
      /*
       * When all the readers have left a terminated node, the writer may proceed; specifically by completing the write
       * future. Unbounded recursion needs to be prevented, however, so completing the future isn't so simple.
       * 
       * To prevent stack growth in recursion we use stack unrolling. The principle mechanism is this: before a release
       * triggers the potential recursion (completing the write future), the triggering node will record its current
       * thread; if nested calls find that their thread is the same as their predecessor (the triggering node) then they
       * can unroll and be released in the triggering method. The nested call (a queued node) informs the triggering
       * node of what to unroll by unlinking its predecessor from the queue, taking the place of the triggering node.
       * The triggering method finds that its predecessor's 'next' has been re-linked (i.e. replaced in the queue) so it
       * begins a new trigger on that new node.
       * 
       * You may think it's strange that none of this unrolling code uses conventional thread-safety mechanisms --
       * access to prev could span threads! That's true, but it's correct: there are 2 fields of `this.prev` that are
       * potentially accessed externally, `releaseThread` and `next`.
       * 
       * - Non-null values of releaseThread are only relevant if it's equal to Thread.current(), so if we read a value
       * that's out of date, we don't care unless it's from the current thread -- but the current thread can't be out of
       * date with itself!
       * 
       * - prev's next is only modified under the condition that prev's releaseThread equals Thread.current() -- so the
       * predecessor is on the same thread, there's no thread-safety issue
       */
      this.releaseThread = Thread.currentThread();
      if (this.releaseThread.equals(this.prev.releaseThread)) {
        /*
         * because the threads match, we know our predecessor is in the process of completing its write future. Because
         * 'this' node has arrived at onCleared as well, we need to complete our own future, but not in this stack
         * frame. Instead, relink ourselves in the position that our predecessor currently has, which will mark us as
         * the next node to trigger
         */
        final Node unlink = this.prev;

        unlink.prev.linkNext(this);

        // make other nodes unreachable from this one to break GC chains.
        // the node is set to point to itself instead of null to allow latent references to still traverse the links:
        // they'll find only this terminated epoch, which implies the head node must be re-read for an up-to-date view
        unlink.prev = unlink.next = unlink;
        unlink.releaseThread = null;
      } else {
        /*
         * The sentinel always points to the node whose write future needs to be completed. Initially it points to
         * 'this' but recursive calls can unlink this node to point sentinel at a new node for completion.
         */
        final Node sentinel = this.prev;
        Node toComplete;
        do {
          toComplete = sentinel.next;
          // as above, relink the node to itself to break chains while still allowing traversal
          sentinel.next = sentinel;

          toComplete.writeLockPromiseToken.complete();

        } while (sentinel.next != sentinel);

        // done unrolling. prevent any ABA thread matches by clearing the release thread
        toComplete.releaseThread = null;
        toComplete.prev = null; // assist GC by clearing predecessor pointers
      }
    }

    final boolean tryReadLock() {
      // the read future must be done to indicate no locked predecessors
      // and entry must succeed to indicate no write locks
      return this.readFuture.isDone() && !shouldRejectEntrant(internalEnter());
    }

    final boolean tryWriteLock() {
      // there must be no readers, which includes implicit predecessors, and no writers, together
      // designated as zero; CAS failure indicates read or write acquire by someone else.
      if (compareAndSet(0, TERMINATED)) {
        writerAcquired();
        return true;
      }
      return false;
    }

    @Override
    public final boolean validate() {
      /*
       * a load fence is required here because the operations within the optimistic read don't necessarily create any
       * dependency with the volatile read of the lock's state. Without such a dependency, the JMM permits ordering
       * reads during that optimistic period *after* this call to validate, making the query essentially meaningless.
       * Using a loadfence forces any loads during the optimistic period to occur before this method call, and thus
       * before the lock's state check
       */
      if (!PlatformDependent.loadFence()) {
        // fence ordering is required, simply fail if it's not supported
        return false;
      }

      /*
       * once issued, a stamp will be valid until the writer on that node is allowed to proceed, i.e. the write future
       * completes. isCleared and writeFuture.isDone are equivalent (clearing triggers future completion), but the
       * former saves us a memory indirection
       */
      return !isCleared();
    }
  }

  /**
   * A dummy node which precedes the first real node in the linked queue, allowing stack unrolling on release without
   * special conditions at the tail of the queue.
   */
  static class SentinelNode extends Node {
  }

  private volatile Node head;

  /**
   * Standard constructor to create a new, unoccupied lock
   */
  public AbstractLinkedEpochReadWriteLock() {
    // no (real) predecessor, no implicit reader, init 0 count
    @SuppressWarnings("squid:S1699") // calling an overridable method is the point here
    final Node h = createNode(0);
    @SuppressWarnings({"resource", "squid:S2095"})
    final Node sentinel = new SentinelNode();
    sentinel.linkNext(h);
    this.head = h;
    // readers may proceed immediately if the lock hasn't been acquired
    h.readFuture.accept(h);
  }

  /**
   * An internal constructor used by some implementations which create non-standard initial conditions (e.g.
   * initializing an already acquired lock in named lock implementations).
   * <p>
   * All appropriate initialization is expected to be done by the invoker!
   */
  AbstractLinkedEpochReadWriteLock(final boolean signatureDisambiguatingParameter) {
  }

  /**
   * Get the node at which new writers will attempt to acquire the write lock
   */
  final Node getWriteNode() {
    return this.head;
  }

  /**
   * update the {@link #getWriteNode() write node} with volatile semantics
   */
  final void setWriteNode(final Node node) {
    this.head = node;
  }

  /**
   * Get the node at which new readers will attempt to acquire the read lock
   * <p>
   * Concrete lock implementations can alter their fairness properties by returning a node that isn't the same as the
   * {@link #getWriteNode() write node}
   */
  abstract Node getReadNode();

  /**
   * Create a node with the appropriate concrete behavior for its corresponding rwlock implementation.
   * 
   * @param initialState The lock mechanism's desired initial state of the underlying {@link AbstractSimpleEpoch}
   */
  abstract Node createNode(int initialState);

  @Override
  public final Future<ReadLockToken> acquireReadLock() {
    Node h = getReadNode();
    while (h.shouldRejectEntrant(h.internalEnter())) {
      // writer has locked this node. re-read head until it updates, then attempt again
      final Node old = h;
      do {
        h = getReadNode();
      } while (h == old);
    }
    return h.readFuture;
  }

  /**
   * Attempt to immediately acquire the read lock, returning a populated {@link Optional} if the lock is not currently
   * held by a writer and there are no writers queued in waiting (i.e. this acquire is "fair" and will not barge ahead
   * of waiting writers).
   */
  @Override
  public final Optional<ReadLockToken> tryReadLock() {
    final Node h = getReadNode();
    return h.tryReadLock() ? Optional.of(h) : Optional.empty();
  }

  @Override
  public final Future<WriteLockToken> acquireWriteLock() {
    Node h = getWriteNode();
    while (h.shouldRejectTerminator(h.internalTerminate())) {
      // some other writer has locked this node. re-read head until it updates, then attempt again
      final Node old = h;
      do {
        h = getWriteNode();
      } while (h == old);
    }
    /*
     * we've successfully write locked a node, so we must push a new node to head that will block subsequent access. We
     * must also track this new node as our successor in order to trigger their readers/writers when we release. Lastly,
     * the node must start with count 1 so that we block its potential write acquisition by being an implicit reader
     */
    setWriteNode(h.linkNext(createNode(1)));
    return h.writeLockPromiseToken;
  }

  @Override
  public final Optional<WriteLockToken> tryWriteLock() {
    final Node h = getWriteNode();
    if (h.tryWriteLock()) {
      setWriteNode(h.linkNext(createNode(1)));
      return Optional.of(h.writeLockPromiseToken);
    } else {
      return Optional.empty();
    }
  }
}
