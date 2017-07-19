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
// Date: Apr 10, 2016
// ---------------------

package com.ibm.async_util.locks;

import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.ibm.async_util.util.StageSupport;


/**
 * An implementation of the {@link AsyncNamedReadWriteLock} interface which enforces fair
 * acquisitions (i.e. readers will queue behind waiting writers to prevent writer starvation).
 * <p>
 * {@code null} values are not permitted for use as names.
 */
public class FairAsyncNamedReadWriteLock<T> implements AsyncNamedReadWriteLock<T> {
  /*
   * Locks are maintained as a linked list (with each link representing a write-lock acquisition),
   * in the same manner as the non-named FairAsyncReadWriteLock implementation, with atomically
   * swapped references to the head in the name map.
   *
   * Nodes can be removed from the map when their internal count reaches zero; they are always
   * initialized with non-zero state. Any acquisition that encounters a zero node in the map must
   * wait for this node to be removed (where the release that brings the node to zero is tasked with
   * removing it from the map)
   */
  private final ConcurrentHashMap<T, AtomicReference<NamedNode>> map = new ConcurrentHashMap<>();

  @SuppressWarnings("serial")
  private final class NamedNode
      extends FairAsyncReadWriteLock.Node {
    /**
     *
     */
    private static final long serialVersionUID = -4539914522341330733L;
    private final T name;

    /**
     * Constructor for the first node in the lock queue (no predecessor)
     */
    NamedNode(final T name, final int initState) {
      super(initState);
      this.name = name;
    }

    /**
     * Constructor for successor nodes in the lock queue (known predecessor)
     */
    NamedNode(final T name, final int initState, final NamedNode prev) {
      super(initState, prev);
      this.name = name;
    }

    /**
     * A node with a value of 0 has been released by all readers and all preceding writers (via
     * implicit read-lock release). The last releaser is tasked with removing this node from the map
     * and thus is only accessible briefly; readers and writers that encounter such a node may not
     * acquire it and must attempt inserting a new head node in the map
     */
    @Override
    void onExit(final int state) {
      if (state == 0) {
        // we're the last releaser on this node. Because the lock bit isn't set, we must also be the
        // head node; since we're last out of the head, we can clear the map of this name
        final AtomicReference<NamedNode> removed =
            FairAsyncNamedReadWriteLock.this.map.remove(this.name);

        /*
         * the name must exist in the map because no other reader/writer is permitted to remove it
         * (only the zero state). If we don't find it, then the name's hashCode has changed since
         * the time of acquisition (or name.equals(name) is false)
         */
        if (removed == null) {
          throw new ConcurrentModificationException(
              "lock name's hashCode or equality has changed since time of acquisition:"
                  + this.name);
        }
        assert removed.get() == this;
      }
    }

    /**
     * @see #onExit(int)
     */
    @Override
    boolean isSpecialEnterState(final int state) {
      return state == 0;
    }

    /**
     * @see #onExit(int)
     */
    @Override
    boolean isSpecialTerminateState(final int state) {
      return state == 0;
    }
  }

  @Override
  public CompletionStage<AsyncReadWriteLock.ReadLockToken> acquireReadLock(final T name) {
    // use this reference box to check whether our computeIfAbsent call did perform the compute
    final NodeBox<NamedNode> created = new NodeBox<>();

    findHead: while (true) {
      created.ref = null;
      final AtomicReference<NamedNode> headRef = computeIfAbsentReader(name, created);

      if (headRef == created.ref) {
        // no one was holding the lock, so we've initialized a read-acquired node and inserted it
        // into the map. good to return
        return StageSupport.completedStage(created.node);
      }

      readHead: while (true) {
        final NamedNode head = headRef.get();
        switch (head.internalEnter()) {
          case SPECIAL:
            // the lock has been released and its releaser is in the process of removing it from the
            // map. loop back and attempt computeIfAbsent, but first yield to give releaser time
            Thread.yield();
            continue findHead;
          case TERMINATED:
            // this node has been write-locked, we can't enter as a reader.
            // re-read head to find the new node created by the write-lock acquirer
            continue readHead;
          case NORMAL:
            // successfully incremented reader count on this node, good to return
            return head.readFuture;
        }
      }
    }
  }

  @Override
  public CompletionStage<AsyncReadWriteLock.WriteLockToken> acquireWriteLock(final T name) {
    final NodeBox<NamedNode> created = new NodeBox<>();

    findHead: while (true) {
      created.ref = null;
      final AtomicReference<NamedNode> headRef = computeIfAbsentWriter(name, created);

      if (headRef == created.ref) {
        // no one was holding the lock, so we've initialized a new head node that is ready to block
        // new readers and writers. here we create our own node as head's predecessor
        final NamedNode head = created.node;
        final NamedNode pred = new NamedNode(name, AbstractSimpleEpoch.TERMINATED);
        pred.next = head;
        head.prev = pred;
        return StageSupport.completedStage(pred);
      }

      readHead: while (true) {
        final NamedNode head = headRef.get();
        switch (head.internalTerminate()) {
          case SPECIAL:
            Thread.yield();
            continue findHead;
          case TERMINATED:
            // this node has been write-locked by someone else, we can't acquire it ourselves.
            // re-read head to find the new node created by the write-lock acquirer
            continue readHead;
          case NORMAL:
            // successfully set lock bit on this node, need to replace head with successor
            final NamedNode newHead = new NamedNode(name, 1, head);
            head.next = newHead;
            headRef.set(newHead);

            return head.writeFuture;
        }
      }
    }
  }

  @Override
  public Optional<AsyncReadWriteLock.ReadLockToken> tryReadLock(final T name) {
    final NodeBox<NamedNode> created = new NodeBox<>();
    final AtomicReference<NamedNode> headRef = computeIfAbsentReader(name, created);

    if (headRef == created.ref) {
      // no node existed in the map, so our computed node was inserted
      return Optional.of(created.node);
    } else {
      final NamedNode h = headRef.get();
      return h.tryReadLock() ? Optional.of(h) : Optional.empty();
    }
  }

  @Override
  public Optional<AsyncReadWriteLock.WriteLockToken> tryWriteLock(final T name) {
    final NodeBox<NamedNode> created = new NodeBox<>();
    final AtomicReference<NamedNode> headRef = computeIfAbsentWriter(name, created);

    if (headRef == created.ref) {
      final NamedNode head = created.node;
      final NamedNode pred = new NamedNode(name, AbstractSimpleEpoch.TERMINATED);
      pred.next = head;
      head.prev = pred;
      return Optional.of(pred);
    } else {
      // the map entry already exists, so someone holds either a reader or writer lock
      return Optional.empty();
    }
  }

  private AtomicReference<NamedNode> computeIfAbsentReader(final T name,
      final NodeBox<NamedNode> box) {
    return this.map.computeIfAbsent(name, keyName -> {
      /*
       * Readers inserting into the map simply create a node with an initial count of 1 and complete
       * the read future. Subsequent acquisitions will operate on this node, either incrementing the
       * state as readers or terminating the node as writers
       */
      final NamedNode node = new NamedNode(keyName, 1);
      node.readFuture.complete(node);

      final AtomicReference<NamedNode> ref = new AtomicReference<>();
      ref.lazySet(box.node = node);
      box.ref = ref;
      return ref;
    });
  }

  private AtomicReference<NamedNode> computeIfAbsentWriter(final T name,
      final NodeBox<NamedNode> box) {
    return this.map.computeIfAbsent(name, keyName -> {
      /*
       * For writers, the initial node inserted into the map is the successor to the node actually
       * returned to the request -- other readers/writers would compete on the successor returned to
       * the first acquisition, so there's no sense in setting the head and immediately changing it.
       * A writer's successor must always begin with a state of 1, which is the writer's implicit
       * read lock; this is how other writers are prevented from proceeding themselves.
       */
      final AtomicReference<NamedNode> ref = new AtomicReference<>();
      ref.lazySet(box.node = new NamedNode(keyName, 1, null));
      box.ref = ref;
      return ref;
    });
  }

  boolean isEmpty() {
    return this.map.isEmpty();
  }

  private static final class NodeBox<E> {
    E node = null;
    AtomicReference<E> ref = null;
  }
}
