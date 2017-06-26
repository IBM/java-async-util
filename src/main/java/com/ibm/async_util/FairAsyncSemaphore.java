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
// @author: renar
//
// Date: Jul 22, 2016
// ---------------------

package com.ibm.async_util;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * An {@link AsyncSemaphore} implementation which is strictly fair: if the permits requested by an
 * {@link #acquire(long)} cannot be fulfilled immediately, all subsequent acquires must wait for
 * their predecessor to be fulfilled. This is the same behavior as a synchronous
 * {@link java.util.concurrent.Semaphore Semaphore} with the fairness parameter set to true.
 * <p>
 * The internal permit representation employs fewer than 64 bits to store the (possibly negative)
 * permit count; as a result, the bounds of available permits are restricted. Methods which take a
 * number of permits as an argument are generally restricted to a maximum value of
 * {@link FairAsyncSemaphore#MAX_PERMITS}. Similarly, the initial permit value provided to the
 * constructor must be at least as great as the defined {@link FairAsyncSemaphore#MIN_PERMITS} value
 */
public class FairAsyncSemaphore implements AsyncSemaphore {

  /*
   * This AsyncSemaphore's implementation consists of a series of linked nodes which each hold some
   * count of permits. The sum of the permits across the nodes denotes the (possibly negative) count
   * of permits in the semaphore.
   * 
   * Initially, the semaphore begins with 1 node, populated with the value given by the constructor
   * parameter. This node is set as the head and tail for the internal FIFO queue. If the semaphore
   * has positive permits, there can only be 1 valid (non-marked-zero, see below) node within the
   * queue; this is an invariant to support the fairness policy, where a node with negative permits
   * must be fulfilled before positive permits may sit idle. If such a positive node has enough
   * permits to satisfy an incoming acquisition, that acquisition will atomically subtract its
   * request from the node's permits and return a complete future.
   * 
   * In general, acquisitions begin at the tail of the queue. If the tail node does not have
   * sufficient permits to satisfy an incoming acquisition, the acquisition will atomically subtract
   * its permits (yielding a negative permit count) and set a mark bit on the node's permit value.
   * This mark bit serves to uniquely claim that node for the acquisition; other acquire calls may
   * not subtract from the marked node's permits, and the node's future is exclusively reserved for
   * a single caller. Once the mark bit is set on a node it may never be unset. Upon marking a node,
   * the acquisition must then insert a new (unmarked) zero-value node onto the tail for subsequent
   * acquisitions and releases to process. Consequently, a marked node must have a successor; this
   * observations raises 2 points:
   * 
   * 1) an acquisition may find the tail node is marked, because marking and inserting a successor
   * are not performed atomically. This acquisition may spin in wait for the new node to be
   * installed as successor to the marked node
   * 
   * 2) a release cannot restore the permit count of a marked node above zero, because there must be
   * a successor (though possibly not a marked one). To enforce fairness, the release's permits must
   * continue to fulfill any remaining nodes in the queue after bringing a marked node up to zero.
   * 
   * Releases begin traversal at the head of the queue. If a marked node is encountered, the
   * releaser adds from its permits to the node's permit deficit (up to zero, as mentioned in (2)).
   * Any remaining permits are used to continue traversal from the successor node. If a node's value
   * is brought up to zero, the node's future is triggered at the end of the release traversal.
   * Marked nodes with zero permits, or ones brought up to zero, are removed from the queue by
   * advancing the head beyond that node. If an unmarked node is encountered, its permits may be
   * incremented freely as it cannot have any successors and traversal may end.
   * 
   * The head and tail references are not required to be strictly maintained. A number of steps may
   * be necessary to reach the first relevant node in the direction of traversal from head or tail,
   * similar to ConcurrentLinkedQueue. Likewise, it is possible for tail to lag behind head.
   * 
   * Marking a node's value is accomplished by setting the high bit of the long. In order to express
   * permits of both positive and negative values in the lower 63 bits, an encoding process is used
   * to translate the range [0, 9223372036854775807] into a corresponding range with negative
   * values, [-4611686018427387904, 4611686018427387903]. This halves the number of possible
   * available permits in the semaphore, but is preferred over the alternative solution of
   * atomically swapping a boxed (boolean, long) pair which would introduce undesirable indirection
   * and object allocation for every update. The encoding process is simply addition of a constant,
   * with decoding performed by applying a mask to eliminate the high bit and then subtracting the
   * same encoding constant. Because the transformation is addition, when the marked bit is
   * disregarded, non-encoded values may safely be added to or subtracted from encoded values
   * without any intermediate decoding. Similarly, comparing encoded values against one another is
   * equivalent to comparing their non-encoded counterparts (disregarding the marked bit). This is
   * useful in comparing against the encoded value of zero.
   * 
   * Stack unrolling of the triggered-by-release futures is accomplished using a ThreadLocal list
   * container. The first call to release on the given thread stack will populate the thread local,
   * noting that the list is empty to indicate that it is the first releaser on the thread. This
   * "thread leader" then adds all futures from fulfilled nodes during its traversal to the list.
   * After traversal, the leader begins triggering futures from this list in insertion order. If any
   * observers also trigger a recursive release on the same thread, that method call will find this
   * same list from the ThreadLocal container and insert its futures on the end. Because the inner
   * call did not receive an empty list, it is not the thread leader and thus does not perform any
   * triggering itself. The method may then return and the thread leader continues triggering
   * futures once the stack unwinds.
   */

  private static final long MARK_BIT = 0x8000000000000000L;

  private static final long ENCODE_OFFSET = MARK_BIT >>> 1L;
  private static final long ENCODE_MASK = MARK_BIT - 1L;

  private static final long ENCODED_MAX = ENCODE_MASK;
  private static final long ENCODED_ZERO = ENCODE_OFFSET;
  private static final long ENCODED_MIN = 0L;

  public static final long MAX_PERMITS = FairAsyncSemaphore.decode(ENCODED_MAX);
  public static final long MIN_PERMITS = FairAsyncSemaphore.decode(ENCODED_MIN);

  // specifically array list in order to imply indexablility, used in manually avoiding
  // ConcurrentModificationException
  private static final ThreadLocal<ArrayList<CompletableFuture<Void>>> UNROLL_FUTURES =
      ThreadLocal.withInitial(ArrayList::new);

  private static final AtomicReferenceFieldUpdater<FairAsyncSemaphore, Node> HEAD_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FairAsyncSemaphore.class, Node.class, "head");
  private static final AtomicReferenceFieldUpdater<FairAsyncSemaphore, Node> TAIL_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FairAsyncSemaphore.class, Node.class, "tail");

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
    FairAsyncSemaphore.checkPermitsFullBounds(initialPermits);
    this.head = this.tail = new Node(FairAsyncSemaphore.encode(initialPermits));
  }

  private static boolean checkPermitsFullBounds(final long permits) {
    return FairAsyncSemaphore.checkPermits(permits, MIN_PERMITS, MAX_PERMITS);
  }

  private static boolean checkPermitsPositiveBounds(final long permits) {
    return FairAsyncSemaphore.checkPermits(permits, 1L, MAX_PERMITS);
  }

  /**
   * returns boolean for use in assertions; true iff valid, throws otherwise
   */
  private static boolean checkPermits(final long permits, final long lowerBound,
      final long upperBound) {
    if (permits < lowerBound || permits > upperBound) {
      throw new IllegalArgumentException(
          String.format("permits must be within [%d, %d], given %d", lowerBound, upperBound,
              permits));
    }
    return true;
  }

  private static long encode(final long permits) {
    assert FairAsyncSemaphore.checkPermitsFullBounds(permits);
    return permits + ENCODE_OFFSET;
  }

  private static long decode(final long encodedPermits) {
    return (encodedPermits & ENCODE_MASK) - ENCODE_OFFSET;
  }

  private Node successorSpin(final Node n) {
    assert (n.getValue() & MARK_BIT) != 0L : "Only marked nodes can have successors";
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
   * 
   * @param permits The number of permits to acquire. This value must be greater than {@code 0} and
   *        no greater than {@link #MAX_PERMITS}
   * @see AsyncSemaphore#acquire(long)
   */
  @Override
  public final CompletionStage<Void> acquire(final long permits) {
    FairAsyncSemaphore.checkPermitsPositiveBounds(permits);
    final Node t = this.tail;
    Node node = t;
    while (true) {
      final long nodeEncodedPermits = node.getValue();
      if ((nodeEncodedPermits & MARK_BIT) != 0L) {
        assert FairAsyncSemaphore
            .decode(nodeEncodedPermits) <= 0L : "Mark bit cannot be set with positive permits";
        // this node has been marked i.e. someone is in the process of updating tail
        node = successorSpin(node);
      } else {
        // node wasn't marked; the future can be claimed if there aren't enough permits

        // subtract the requested permits entirely; if the result is non-negative, we can return
        // immediately. if negative, set mark bit then queue a new zero node to receive new acquires
        final long encodedDiff = nodeEncodedPermits - permits;
        if (encodedDiff >= ENCODED_ZERO) {
          // enough permits to satisfy request, attempt CAS
          if (node.casValue(nodeEncodedPermits, encodedDiff)) {
            return FutureSupport.voidFuture();
          }
        } else {
          // not enough permits to satisfy request. CAS to deficit+mark then enqueue new zero
          if (node.casValue(nodeEncodedPermits, encodedDiff | MARK_BIT)) {
            final Node newNode = new Node(ENCODED_ZERO);
            node.setNext(newNode);
            updateTail(t, newNode);
            return node.getFuture();
          }
        }
      }
      // arriving here indicates CAS failure or updated successor node
      // loop back and reread the node's value
    }
  }

  /**
   * @param permits The number of permits to release. This value must be greater than {@code 0} and
   *        no greater than {@link #MAX_PERMITS}
   */
  @Override
  public final void release(long permits) {
    FairAsyncSemaphore.checkPermitsPositiveBounds(permits);

    // lazily initialized list of threadlocal futures to complete
    ArrayList<CompletableFuture<Void>> toComplete = null;
    boolean threadLeader = false;
    final Node h = this.head;
    Node node = h;
    while (true) {
      final long nodeEncodedPermits = node.getValue();
      if ((nodeEncodedPermits & MARK_BIT) != 0L) {
        // node is marked, so we can't increment above zero
        assert FairAsyncSemaphore
            .decode(nodeEncodedPermits) <= 0L : "Mark bit cannot be set with positive permits";
        if (nodeEncodedPermits == (ENCODED_ZERO | MARK_BIT)) {
          // skip zero+marked nodes
          node = successorSpin(node);
        } else {
          // only operate if there is a permit deficit (zero+mark nodes have already been claimed)

          final long encodedDiff = nodeEncodedPermits + permits;
          // here we decode because the mark bit is set
          final long diff = FairAsyncSemaphore.decode(encodedDiff);
          if (diff < 0L) {
            // resulting permits on this node are still negative, so we can CAS and stop advancing
            if (node.casValue(nodeEncodedPermits, encodedDiff)) {
              break;
            }
          } else {
            // the released permits will release the node
            // it cannot, however, exceed zero because the node is marked (it has a queued
            // successor)
            if (node.casValue(nodeEncodedPermits, ENCODED_ZERO | MARK_BIT)) {
              if (toComplete == null) {
                assert !threadLeader;
                toComplete = UNROLL_FUTURES.get();
                threadLeader = toComplete.isEmpty();
              }
              toComplete.add(node.getFuture());
              if (diff == 0L) {
                // we're out of permits to restore, done
                break;
              }
              permits = diff;
              // advance to next node to continue releasing
              node = successorSpin(node);
            }
          }
        }
      } else {
        // node is unmarked, we can release all permits
        if (ENCODED_MAX - permits < nodeEncodedPermits) {
          // overflow conscious limit check
          throw new IllegalStateException(
              String.format("Exceeded maximum allowed semaphore permits: %d + %d > %d",
                  FairAsyncSemaphore.decode(nodeEncodedPermits), permits, MAX_PERMITS));
        }
        final long encodedUpdate = nodeEncodedPermits + permits;
        if (node.casValue(nodeEncodedPermits, encodedUpdate)) {
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
      UNROLL_FUTURES.remove();
    }
  }

  /**
   * Attempts to acquire the given number of permits if they are immediately available. This attempt
   * honors the fairness policy; if any acquisitions are queued, this method will fail immediately.
   * <p>
   * Note that this differs from the behavior of
   * {@link java.util.concurrent.Semaphore#tryAcquire(int)} with a fairness policy. This method will
   * <i>not</i> barge ahead of other waiters.
   * 
   * @param permits The number of permits to acquire. This value must be greater than {@code 0} and
   *        no greater than {@link #MAX_PERMITS}
   * @see AsyncSemaphore#tryAcquire(long)
   */
  @Override
  public final boolean tryAcquire(final long permits) {
    FairAsyncSemaphore.checkPermitsPositiveBounds(permits);

    final Node h = this.head;
    long nodeEncodedPermits, encodedDiff;
    do {
      nodeEncodedPermits = h.getValue();
      if ((nodeEncodedPermits & MARK_BIT) != 0) {
        // head is marked, can't acquire
        return false;
      }

      encodedDiff = nodeEncodedPermits - permits;
      if (encodedDiff < ENCODED_ZERO) {
        // not enough permits to satisfy acquire
        return false;
      }
    } while (!h.casValue(nodeEncodedPermits, encodedDiff));
    return true;
  }

  @Override
  public final long drainPermits() {
    final Node h = this.head;
    long nodeEncodedPermits;
    do {
      nodeEncodedPermits = h.getValue();
      if (nodeEncodedPermits <= ENCODED_ZERO) {
        // permits are non-positive (possibly marked). can't drain anything
        return 0L;
      }
    } while (!h.casValue(nodeEncodedPermits, ENCODED_ZERO));

    return FairAsyncSemaphore.decode(nodeEncodedPermits);
  }

  @Override
  public final long getAvailablePermits() {
    long sum = 0L;
    for (Node n = this.head; n != null; n = n.getNext()) {
      sum += FairAsyncSemaphore.decode(n.getValue());
    }
    return sum;
  }

  @Override
  public final int getQueueLength() {
    int count = 0;
    for (Node n = this.head; n != null; n = n.getNext()) {
      final long encodedPermits = n.getValue();
      if ((encodedPermits & MARK_BIT) != 0L && FairAsyncSemaphore.decode(encodedPermits) < 0L) {
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
      sum += FairAsyncSemaphore.decode(n.getValue());
      count++;
    }
    return "FairAsyncSemaphore [permits=" + sum +
        ", nodes=" + count +
        ", head=" + h +
        ", tail=" + t +
        "]";
  }

  @SuppressWarnings("serial")
  private static final class Node extends AtomicLong {
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private volatile Node next;

    Node(final long initialValue) {
      // lazy because it isn't visible until the node is written to a volatile ref itself
      lazySet(initialValue);
    }

    long getValue() {
      return get();
    }

    boolean casValue(final long expected, final long update) {
      return compareAndSet(expected, update);
    }

    CompletableFuture<Void> getFuture() {
      return this.future;
    }

    void setNext(final Node n) {
      this.next = n;
    }

    Node getNext() {
      return this.next;
    }

    @Override
    public String toString() {
      final long val = getValue();
      return String.format("Node [0x%x (%d)]", val, FairAsyncSemaphore.decode(val));
    }
  }
}
