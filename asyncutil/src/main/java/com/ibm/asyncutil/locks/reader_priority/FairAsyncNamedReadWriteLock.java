/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.cleversafe.util.async.AbstractLinkedEpochReadWriteLock.Node;
import org.cleversafe.util.async.AbstractLinkedEpochReadWriteLock.SentinelNode;

/**
 * An implementation of the {@link AsyncNamedReadWriteLock} interface which enforces fair acquisitions (i.e. readers
 * will queue behind waiting writers to prevent writer starvation).
 * <p>
 * {@code null} values are not permitted for use as names.
 */
public final class FairAsyncNamedReadWriteLock<T> extends AbstractAsyncNamedReadWriteLock<T> {
  private class FairNamedLock extends FairAsyncReadWriteLock {
    private final T name;

    public FairNamedLock(final T name) {
      // initialized by the callers
      super(true);
      this.name = name;
    }

    @Override
    Node createNode(final int initialState) {
      return new FairNamedNode(initialState);
    }

    private class FairNamedNode extends FairNode {
      FairNamedNode(final int initialState) {
        super(initialState);
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
          final AbstractLinkedEpochReadWriteLock removed = removeFromMap(FairNamedLock.this.name);
          assert removed == FairNamedLock.this;
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
  FairAsyncReadWriteLock createReaderAcquiredLock(final T name) {
    final FairNamedLock lock = new FairNamedLock(name);

    // TODO(java11) _all_ of these writes can be plain (links, predecessor, head, future completion)

    @SuppressWarnings({"resource", "squid:S2095"})
    final Node sentinel = new SentinelNode();
    final Node acquiredNode = lock.createNode(1);
    acquiredNode.readFuture.accept(acquiredNode);

    lock.setWriteNode(sentinel.linkNext(acquiredNode));

    return lock;
  }

  @Override
  FairAsyncReadWriteLock createWriterAcquiredLock(final T name) {
    final FairNamedLock lock = new FairNamedLock(name);

    // TODO(java11) _all_ of these writes can be plain (links, predecessor, head, future completion)

    @SuppressWarnings({"resource", "squid:S2095"})
    final Node sentinel = new SentinelNode();
    final Node acquiredNode = lock.createNode(AbstractSimpleEpoch.TERMINATED);
    acquiredNode.writerAcquired();

    lock.setWriteNode(sentinel.linkNext(acquiredNode).linkNext(lock.createNode(1)));

    return lock;
  }

}
