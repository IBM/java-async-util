/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.cleversafe.util.async.AbstractLinkedEpochReadWriteLock.Node;
import org.cleversafe.util.async.AbstractLinkedEpochReadWriteLock.SentinelNode;

/**
 * An implementation of the {@link AsyncNamedReadWriteLock} interface which favors reader acquisitions over write
 * acquisitions, allowing readers to enter at the front of the queue when possible. See
 * {@link ReaderPriorityAsyncReadWriteLock} for details.
 * <p>
 * {@code null} values are not permitted for use as names.
 * 
 * @see AsyncNamedReadWriteLock
 * @see ReaderPriorityAsyncReadWriteLock
 */
public final class ReaderPriorityAsyncNamedReadWriteLock<T> extends AbstractAsyncNamedReadWriteLock<T> {

  private class ReaderPriorityNamedLock extends ReaderPriorityAsyncReadWriteLock {
    private final T name;

    public ReaderPriorityNamedLock(final T name) {
      // initialized by the callers
      super(true);
      this.name = name;
    }

    @Override
    Node createNode(final int initialState) {
      return new ReaderPriorityNamedNode(initialState);
    }

    private class ReaderPriorityNamedNode extends ReaderPriorityNode {

      ReaderPriorityNamedNode(final int initialEntrants) {
        super(initialEntrants);
      }

      /**
       * A node with a value of 0 has been released by all readers and all preceding writers (via implicit read-lock
       * release). The last releaser is tasked with removing this node from the map and thus is only accessible briefly;
       * readers and writers that encounter such a node may not acquire it and must attempt inserting a new head node in
       * the map
       */
      @Override
      void onExit(final int state) {
        if (isBeingRemoved(state)) {
          final AbstractLinkedEpochReadWriteLock removed = removeFromMap(ReaderPriorityNamedLock.this.name);
          assert removed == ReaderPriorityNamedLock.this;
          assert removed.getWriteNode() == this;
        } else {
          super.onExit(state);
        }
      }

      @Override
      boolean shouldRejectEntrant(final int currentState) {
        return isBeingRemoved(currentState) || super.shouldRejectEntrant(currentState);
      }

      @Override
      boolean shouldRejectTerminator(final int currentState) {
        return isBeingRemoved(currentState) || super.shouldRejectTerminator(currentState);
      }

    }
  }

  @Override
  ReaderPriorityAsyncReadWriteLock createReaderAcquiredLock(final T name) {
    final ReaderPriorityNamedLock lock = new ReaderPriorityNamedLock(name);

    // TODO(java11) _all_ of these writes can be plain (links, predecessor, head, future completion)

    // the node before the head is a sentinel for unrolling and the readerPredecessor.
    @SuppressWarnings("squid:S2095")
    final Node headPredecessor = lock.createNode(AbstractSimpleEpoch.TERMINATED);
    // the head node has a count of 1 as a reader acquired node
    final Node newHead = lock.createNode(1);

    newHead.readFuture.accept(newHead); // readers can proceed, so complete the future

    headPredecessor.writerAcquired(); // set the readerPredecessor
    lock.setWriteNode(headPredecessor.linkNext(newHead));

    return lock;
  }

  @Override
  ReaderPriorityAsyncReadWriteLock createWriterAcquiredLock(final T name) {
    final ReaderPriorityNamedLock lock = new ReaderPriorityNamedLock(name);

    // TODO(java11) _all_ of these writes can be plain (links, predecessor, head, future completion)

    // the node before the head is an acquired writer (and the readerPredecessor)
    @SuppressWarnings("squid:S2095")
    final Node headPredecessor = lock.createNode(AbstractSimpleEpoch.TERMINATED);
    // the head node has a count of 1 as a writer's implicit read lock (see notes on the RWLock)
    final Node newHead = lock.createNode(1);

    // the write lock will access the predecessor node, so that node needs a predecessor itself for trampolining
    // this doesn't have to be a named node no one ever accesses it
    @SuppressWarnings({"resource", "squid:S2095"})
    final Node sentinel = new SentinelNode();
    sentinel.linkNext(headPredecessor);

    headPredecessor.writerAcquired(); // set the readerPredecessor
    lock.setWriteNode(headPredecessor.linkNext(newHead));

    return lock;
  }
}
