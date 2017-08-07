/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.ibm.async_util.util.StageSupport;

/**
 * An {@link AsyncSemaphore} implementation which is strictly fair: if the permits requested by an
 * {@link #acquire(long)} cannot be fulfilled immediately, all subsequent acquires must wait for
 * their predecessor to be fulfilled. This is the same behavior as a synchronous
 * {@link java.util.concurrent.Semaphore Semaphore} with the fairness parameter set to true.
 * <p>
 * The internal permit representation may employ fewer than 64 bits to store the (possibly negative)
 * permit count; as a result, the bounds of available permits are restricted. Methods which take a
 * number of permits as an argument are generally restricted to a maximum value of
 * {@link FairAsyncSemaphore#MAX_PERMITS}. Similarly, the initial permit value provided to the
 * constructor must be at least as great as the defined {@link FairAsyncSemaphore#MIN_PERMITS} value
 */
public class FairAsyncSemaphore implements AsyncSemaphore {

  /*
   * This AsyncSemaphore's implementation consists of a series of linked nodes which each hold some
   * count of permits. The sum of the permits across the nodes denotes the (possibly negative) count
   * of permits in the semaphore. If the semaphore has positive permits, these permits all reside on
   * the last node within the queue; this invariant enforces the fairness policy, where a node with
   * negative permits must be fulfilled before positive permits may sit idle.
   * 
   * Initially, the semaphore begins with 1 node, populated with the value given by the constructor
   * parameter. This node is set as the head and tail for the internal FIFO queue.
   * 
   * Acquisitions begin at the tail of the queue. If the encountered node is not yet reserved (see
   * below) the requested permits are atomically subtracted from the node's current permits. If the
   * resulting value is non-negative, the acquisition was successful, and the method returns a
   * completed future. If the result is negative, (i.e. the node does not have sufficient permits to
   * satisfy the incoming acquisition) the node is uniquely reserved for the acquisition; other
   * acquire calls may not subtract from the negative node's permits, and the node's future is
   * exclusively reserved for this single caller. Once a node's permits are negative, they may never
   * be restored to zero or more. The acquisition must then insert a new zero-value node onto the
   * tail for subsequent acquisitions and releases to process. Consequently, a negative node must
   * have a successor; this observations raises 2 points:
   * 
   * 1) an acquisition may find the tail node is negative, because subtracting and inserting a
   * successor are not performed atomically. This acquisition may spin in wait for the new node to
   * be installed as successor to the reserved node
   * 
   * 2) a release cannot restore the permit count of a reserved node to zero or more, because that
   * node must have a successor which may need permits itself. To enforce fairness, the release's
   * permits must continue to fulfill any remaining nodes in the queue after fulfilling a reserved
   * node.
   * 
   * Releases begin traversal at the head of the queue. If a negative node is encountered, the
   * releaser adds from its permits to the node's permit deficit, up to a value of zero. Any
   * remaining permits are used to continue traversal from the successor node. If the deficit is now
   * zero, the permit value is instead marked as COMPLETED, which is a special marker value used to
   * ignore nodes which have completed their lifecycle. These nodes are removed from the queue by
   * advancing the head beyond that node, then completing their associated future. If a positive
   * node is encountered by a releaser, its permits may be incremented freely as it cannot have any
   * successors and traversal may end.
   * 
   * The head and tail references are not required to be strictly maintained. A number of steps may
   * be necessary to reach the first relevant node in the direction of traversal from head or tail,
   * similar to ConcurrentLinkedQueue. Likewise, it is possible for tail to lag behind head.
   * 
   * Without proper precautions, releasing an AsyncSemaphore can cause unbounded recursion if
   * dependents perform only synchronous work and release the same semaphore themselves. Here we
   * prevent this implicit recursion by completing futures with a ThreadLocal trampoline mechanism.
   * The first call to release on the given thread stack will populate the thread local list, noting
   * that the list is empty to indicate that it is the first releaser on the thread. This
   * "thread leader" then adds all futures from fulfilled nodes during its traversal to the list.
   * After traversal, the leader begins triggering futures from this list in insertion order. If any
   * dependents also trigger a recursive release on the same thread, that method call will find this
   * same list from the ThreadLocal container and insert its futures on the end. Because the inner
   * call did not receive an empty list, it is not the thread leader and thus does not perform any
   * triggering itself. The method may then return and the thread leader continues triggering
   * futures once the stack unwinds.
   * 
   * Note that a user could create a permit deficit greater than MIN_PERMITS -- in fact, the deficit
   * can grow arbitrarily large by repeated calls to acquire(MAX_PERMITS), where each node will
   * happily count negative MAX_PERMITS. There are no restrictions in place to prohibit this, and
   * theoretically it is sound, but it's generally discouraged. Methods like getAvailablePermits may
   * throw exceptions in encountering overflow, for example, and separate acquisitions will not be
   * atomic for values exceeding the defined maximums.
   */

  /**
   * finished value for nodes which have been fulfilled, i.e. released from a deficit. must be
   * negative to indicate a non-reservable node
   */
  private static final long COMPLETED = Long.MIN_VALUE;

  /**
   * The greatest deficit of permits with which this semaphore implementation can be initialized.
   */
  public static final long MIN_PERMITS = COMPLETED + 1;

  /**
   * The greatest number of permits with which this semaphore implementation can be initialized and
   * can be {@link #acquire(long) acquired} or {@link #release(long) released} with a single
   * operation.
   */
  /*
   * This value was chosen to meet 2 criteria: 1) 0 - MAX_PERMITS >= MIN_PERMITS, to prevent issues
   * with overflow 2) MAX_PERMITS != Long.MAX_VALUE, because 'acquire(Long.MAX_VALUE)' is a common
   * habit among users who don't read this class's docs, and want to use some absurd value to ensure
   * that no one else acquires the semaphore. There was a time when this implementation did not
   * support acquiring Long.max permits, and we reserve the right to return to that standard if
   * necessary. It's better that a user fails an arg check now and hopefully starts using this
   * field, rather than possibly introducing an arg check failure later into user code that
   * previously worked fine
   */
  public static final long MAX_PERMITS = -MIN_PERMITS - 1;

  private static final ThreadLocal<ArrayList<CompletableFuture<Void>>> TRAMPOLINE_FUTURES =
      ThreadLocal.withInitial(ArrayList::new);

  private static final AtomicReferenceFieldUpdater<FairAsyncSemaphore, Node> HEAD_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FairAsyncSemaphore.class, Node.class, "head");
  private static final AtomicReferenceFieldUpdater<FairAsyncSemaphore, Node> TAIL_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FairAsyncSemaphore.class, Node.class, "tail");

  private static final Node NULL_PREDECESSOR;

  static {
    final Node n = new Node(null, COMPLETED);
    n.getFuture().complete(null);
    NULL_PREDECESSOR = n;
  }

  private volatile Node head, tail;

  /**
   * Creates a new asynchronous semaphore with the given initial number of permits. This value may
   * be negative in order to require {@link #release(long) releases} before {@link #acquire(long)
   * acquisitions}
   * 
   * @param initialPermits The initial number of permits available in the semaphore. This value must
   *        be within the interval [{@link #MIN_PERMITS}, {@link #MAX_PERMITS}]
   */
  public FairAsyncSemaphore(final long initialPermits) {
    if (initialPermits < MIN_PERMITS || initialPermits > MAX_PERMITS) {
      throw new IllegalArgumentException(
          String.format("initial permits must be within [%d, %d], given %d",
              MIN_PERMITS, MAX_PERMITS, initialPermits));
    }

    final Node h = new Node(NULL_PREDECESSOR, initialPermits);

    /*
     * If the initial permits are negative, create a zero node to place at the tail for new
     * acquisitions, and a negative node (with no observer) at the head, which must be restored
     * before new acquisitions can be fulfilled. If the initial permits are non-negative, a single
     * node with this initial value can be placed as the head and the tail
     */
    if (initialPermits < 0L) {
      final Node t = new Node(h);
      h.lazySetNext(t);
      this.tail = t;
    } else {
      this.tail = h;
    }
    this.head = h;
  }

  private static void checkPermitsBounds(final long permits) {
    if (permits < 0L || permits > MAX_PERMITS) {
      throw new IllegalArgumentException(
          String.format("permits must be within [%d, %d], given %d",
              0L, MAX_PERMITS, permits));
    }
  }

  private Node successorSpin(final Node n) {
    assert n.getPermits() < 0L : "Only reserved nodes can have successors";
    Node next;
    while ((next = n.getNext()) == null) {
      // there is a brief period after marking during which successor is null
      // spin until it's populated
    }
    return next;
  }

  private void updateHead(final Node expected, final Node update) {
    // CAS failure is fine, head and tail updates are best-effort.
    // volatile writes aren't tolerable because it would allow regression
    HEAD_UPDATER.compareAndSet(this, expected, update);
  }

  private void updateTail(final Node expected, final Node update) {
    TAIL_UPDATER.compareAndSet(this, expected, update);
  }

  /**
   * Acquires the given number of permits using an acquisition-ordered fair queuing policy, i.e. if
   * all the requested permits cannot be fulfilled immediately, the acquisition will enter a FIFO
   * queue. Any subsequent acquisitions will also enter this queue until the head has been fulfilled
   * by sufficient releases.
   * <p>
   * It is possible to request an acquisition of {@code 0} permits, which in this implementation has
   * a specific meaning: if there are no queued waiters and zero or more permits available, the
   * acquisition will complete immediately; if there are queued waiters or a permit deficit, the
   * zero-acquisition will complete after the last queued waiter (at the time of acquisition) is
   * released. No permits will be reserved by the zero-acquisition in either of these cases. This
   * behavior can be used to wait for pending acquisitions to complete, without affecting the permit
   * count.
   * 
   * @param permits The number of permits to acquire. This value must be non-negative and no greater
   *        than {@link #MAX_PERMITS}
   * @see AsyncSemaphore#acquire(long)
   */
  @Override
  public final CompletionStage<Void> acquire(final long permits) {
    FairAsyncSemaphore.checkPermitsBounds(permits);

    final Node t = this.tail;
    Node node = t;
    while (true) {
      final long nodePermits = node.getPermits();
      if (nodePermits < 0L) {
        // this node has been reserved i.e. someone is in the process of updating tail
        node = successorSpin(node);
      } else if (permits == 0L && nodePermits == 0L) {
        /*
         * In order to acquire zero permits, the acquirer must find whether any waiters are queued,
         * and if so, attach to the last one (i.e. return its future). If it finds a positive permit
         * count at the tail, then all waiters must have been released and the normal CAS path can
         * be used. If the tail is negative, then a successor to that node must exist (tail is
         * stale), so it's not known whether this node is the last one or not, and acquire must
         * advance. If the tail is zero, then the immediately preceding node is the target that
         * we're looking for, and its future must be returned.
         */
        final Node prev = node.getPrevious();
        // x.prev is nulled when x is COMPLETED (so we've encountered a benign race).
        // inherently all predecessors are done if the node is done
        return prev == null ? StageSupport.voidFuture() : prev.getFuture();
      } else {
        // node wasn't reserved; the future can be claimed if there aren't enough permits

        // subtract the requested permits; if the result is non-negative, we can return
        // immediately. if negative, enqueue a new zero node to receive new acquires
        final long diff = nodePermits - permits;
        if (node.casValue(nodePermits, diff)) {
          if (diff < 0L) {
            // not enough permits to satisfy request
            final Node newNode = new Node(node);
            node.setNext(newNode);
            updateTail(t, newNode);
            return node.getFuture();
          } else {
            return StageSupport.voidFuture();
          }
        }
      }
      // arriving here indicates CAS failure or updated successor node
      // loop back and reread the node's value
    }
  }

  /**
   * @param permits The number of permits to release. This value must be non-negative and no greater
   *        than {@link #MAX_PERMITS}
   */
  @Override
  public final void release(long permits) {
    FairAsyncSemaphore.checkPermitsBounds(permits);

    // lazily initialized list of threadlocal futures to complete
    ArrayList<CompletableFuture<Void>> toComplete = null;
    boolean threadLeader = false;
    final Node h = this.head;
    Node node = h;

    while (true) {
      final long nodePermits = node.getPermits();
      if (nodePermits < 0L) {
        // node is reserved, so we might be able to fulfill it
        if (nodePermits == COMPLETED) {
          // skip completed nodes, nothing to do
          node = successorSpin(node);
        } else {
          final long sum = nodePermits + permits;
          if (sum < 0L) {
            // resulting permits on this node are still negative, so we can CAS and stop advancing
            if (node.casValue(nodePermits, sum)) {
              break;
            }
          } else {
            // the released permits will release the node
            // it must remain negative, however, because only the single newly-created tail node may
            // be non-negative to receive acquisitions
            if (node.casValue(nodePermits, COMPLETED)) {
              node.setPrev(null); // unset prev to break chains for GC
              if (toComplete == null) {
                assert !threadLeader;
                toComplete = TRAMPOLINE_FUTURES.get();
                threadLeader = toComplete.isEmpty();
              }
              toComplete.add(node.getFuture());
              if (sum == 0L) {
                // we're out of permits to restore, done
                break;
              }
              permits = sum;
              // advance to next node to continue releasing
              node = successorSpin(node);
            }
          }
        }
      } else {
        // node is not reserved, we can release all permits
        if (MAX_PERMITS - permits < nodePermits) {
          // overflow conscious limit check
          throw new IllegalStateException(
              String.format("Exceeded maximum allowed semaphore permits: %d + %d > %d",
                  nodePermits, permits, MAX_PERMITS));
        }
        if (node.casValue(nodePermits, nodePermits + permits)) {
          break;
        }
      }
      // either CAS failure or updated successor node, reread permits
    }

    // nodes have been updated, proceed with cleanup

    if (node != h) {
      // update the head if we advanced
      updateHead(h, node);
    }

    if (threadLeader) {
      // this is the first release call in the thread's stack, perform the unrolling
      if (toComplete == null) {
        throw new AssertionError();
      }

      // explicitly iterate because the list might be modified by implicit future recursion
      for (int i = 0; i < toComplete.size(); i++) {
        toComplete.get(i).complete(null);
      }
      // we're done unrolling, clear this thread of futures
      TRAMPOLINE_FUTURES.remove();
    }
  }

  /**
   * Attempts to acquire the given number of permits if they are immediately available. This attempt
   * honors the fairness policy; if any acquisitions are queued, this method will fail immediately.
   * <p>
   * Note that this differs from the behavior of
   * {@link java.util.concurrent.Semaphore#tryAcquire(int)} with a fairness policy. This method will
   * <i>not</i> barge ahead of other waiters.
   * <p>
   * As with {@link #acquire(long)}, it is possible to {@code tryAcquire} with zero permits. This
   * will return {@code true} iff there are no queued waiters and zero or more permits are available
   * in the semaphore.
   * 
   * @param permits The number of permits to acquire. This value must be non-negative and no greater
   *        than {@link #MAX_PERMITS}
   * @see AsyncSemaphore#tryAcquire(long)
   */
  @Override
  public final boolean tryAcquire(final long permits) {
    FairAsyncSemaphore.checkPermitsBounds(permits);

    final Node h = this.head;
    Node node = h;
    while (true) {
      final long nodePermits = node.getPermits();

      if (nodePermits == COMPLETED) {
        // stale head, advance to something useful
        node = successorSpin(node);
        continue;
      }

      final long diff;
      if (nodePermits < 0L
          || (diff = nodePermits - permits) < 0L) {
        // head is reserved
        // or not enough permits to satisfy acquire
        return false;
      }

      if (node.casValue(nodePermits, diff)) {
        break;
      }
    }

    if (h != node) {
      updateHead(h, node);
    }

    if (permits == 0L) {
      // special case for zero permits, perform the queued waiters check
      final Node prev = node.getPrevious();
      return prev == null || prev.getPermits() == COMPLETED;
    }

    return true;
  }

  @Override
  public final long drainPermits() {
    final Node h = this.head;
    long nodePermits;
    do {
      nodePermits = h.getPermits();
      if (nodePermits <= 0L) {
        // permits are non-positive (possibly reserved). can't drain anything
        return 0L;
      }
    } while (!h.casValue(nodePermits, 0));

    return nodePermits;
  }

  @Override
  public final long getAvailablePermits() {
    long sum = 0L;
    for (Node n = this.head; n != null; n = n.getNext()) {
      final long permits = n.getPermits();
      if (permits != COMPLETED) {
        sum = Math.addExact(sum, permits);
      }
    }
    return sum;
  }

  @Override
  public final int getQueueLength() {
    int count = 0;
    for (Node n = this.head; n != null; n = n.getNext()) {
      final long permits = n.getPermits();
      if (permits < 0L && permits != COMPLETED) {
        count++;
      }
    }
    return count;
  }

  @Override
  public String toString() {
    final Node h = this.head;
    final Node t = this.tail;
    int count = 0;
    long sum = 0L;
    for (Node n = h; n != null; n = n.getNext()) {
      final long permits = n.getPermits();
      if (permits != COMPLETED) {
        sum = Math.addExact(sum, permits);
      }
      count++; // include completed nodes for count
    }
    return "FairAsyncSemaphore [permits=" + sum +
        ", nodes=" + count +
        ", head=" + h +
        ", tail=" + t +
        "]";
  }

  @SuppressWarnings("serial")
  private static final class Node extends AtomicLong {
    private static final AtomicReferenceFieldUpdater<Node, Node> NEXT_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private volatile Node next;
    private Node prev;

    Node(final Node prev) {
      this.prev = prev;
    }

    Node(final Node prev, final long initialValue) {
      this.prev = prev;
      // lazy because it isn't visible until the node is written to a volatile ref itself
      lazySet(initialValue);
    }

    long getPermits() {
      return get();
    }

    boolean casValue(final long expected, final long update) {
      return compareAndSet(expected, update);
    }

    CompletableFuture<Void> getFuture() {
      return this.future;
    }

    void lazySetNext(final Node n) {
      NEXT_UPDATER.lazySet(this, n);
    }

    void setNext(final Node n) {
      this.next = n;
    }

    Node getNext() {
      return this.next;
    }

    void setPrev(final Node n) {
      this.prev = n;
    }

    Node getPrevious() {
      return this.prev;
    }

    @Override
    public String toString() {
      return "Node [" + getPermits() + "]";
    }
  }
}
