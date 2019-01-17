/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.cleversafe.util.async.AbstractLinkedEpochReadWriteLock.Node;
import org.cleversafe.util.async.AsyncReadWriteLock.ReadLockToken;
import org.cleversafe.util.async.AsyncReadWriteLock.WriteLockToken;

/**
 * This abstract class forms the basis of {@link AsyncNamedReadWriteLock} implementations built on the
 * {@link AbstractLinkedEpochReadWriteLock}
 * <p>
 * {@code null} values are not permitted for use as names.
 * 
 * @param <T> The type of the lock identifiers
 */
// labels and continues make the retry simpler than alternatives
@SuppressWarnings({"squid:LabelsShouldNotBeUsedCheck", "squid:S135", "squid:S3626"})
abstract class AbstractAsyncNamedReadWriteLock<T> implements AsyncNamedReadWriteLock<T> {
  /*
   * Locks are maintained with the same internal structure as their foundational class, though the actual methods for
   * acquiring nodes are not called by this class. Locks are acquired either at the time of their construction (under
   * protection of the map's computeIfAbsent), or by following the epoch implementation calls.
   * 
   * Nodes can be removed from the map when their internal count reaches zero; they are always initialized with non-zero
   * state. Any acquisition that encounters a zero node in the map must wait for this node to be removed (where the
   * release that brings the node to zero is tasked with removing it from the map)
   */

  /**
   * checks whether the given epoch state implies that the node is currently being removed from the map (and should not
   * accept new acquisitions)
   */
  static boolean isBeingRemoved(final int currentState) {
    return currentState == 0;
  }

  private final ConcurrentHashMap<T, AbstractLinkedEpochReadWriteLock> map = new ConcurrentHashMap<>();

  /**
   * Remove the lock with the given name from the map. This should be called by nodes when they have reached a stable
   * state of zero entrants
   * 
   * @return the lock removed from the map
   */
  final AbstractLinkedEpochReadWriteLock removeFromMap(final T name) {
    // we're the last releaser on this node. Because the lock bit isn't set, we must also be the
    // head node; since we're last out of the head, we can clear the map of this name
    final AbstractLinkedEpochReadWriteLock removed = this.map.remove(name);

    /*
     * the name must exist in the map because no other reader/writer is permitted to remove it (only the zero state). If
     * we don't find it, then the name's hashCode has changed since the time of acquisition (or name.equals(name) is
     * false)
     */
    if (removed == null) {
      throw new ConcurrentModificationException(
          "lock name's hashCode or equality has changed since time of acquisition:"
              + name);
    }
    return removed;
  }

  /**
   * Create a lock to hold nodes. This lock should be initialized with a reader already holding the head node, with the
   * given name identifying the lock (for use by {@link #removeFromMap(Object)})
   */
  abstract AbstractLinkedEpochReadWriteLock createReaderAcquiredLock(T name);

  /**
   * Create a lock to hold nodes. This lock should be initialized with a writer already holding a node, with the given
   * name identifying the lock (for use by {@link #removeFromMap(Object)}). This node must be the immediate
   * {@link Node#prev predecessor} of the head node
   */
  abstract AbstractLinkedEpochReadWriteLock createWriterAcquiredLock(T name);

  @Override
  public Future<ReadLockToken> acquireReadLock(final T name) {
    // use this reference box to check whether our computeIfAbsent call did perform the compute
    final NodeBox created = new NodeBox();

    findHead: while (true) {
      created.lock = null;
      final AbstractLinkedEpochReadWriteLock foundLock = computeIfAbsentReader(name, created);

      if (foundLock == created.lock) {
        // no one was holding the lock, so we've initialized a read-acquired node and inserted it
        // into the map. good to return
        return Futures.of(created.node);
      }

      readHead: while (true) {
        final Node head = foundLock.getReadNode();
        final int previousState = head.internalEnter();
        if (isBeingRemoved(previousState)) {
          // the lock has been released and its releaser is in the process of removing it from the
          // map. loop back and attempt computeIfAbsent, but first yield to give releaser time
          Thread.yield();
          continue findHead;
        } else if (head.shouldRejectEntrant(previousState)) {
          // this node has been write-locked, we can't enter as a reader.
          // re-read head to find the new node created by the write-lock acquirer
          continue readHead;
        } else {
          // successfully incremented reader count on this node, good to return
          return head.readFuture;
        }
      }
    }
  }

  @Override
  public Future<WriteLockToken> acquireWriteLock(final T name) {
    final NodeBox created = new NodeBox();

    findHead: while (true) {
      created.lock = null;
      final AbstractLinkedEpochReadWriteLock foundLock = computeIfAbsentWriter(name, created);

      if (foundLock == created.lock) {
        // no one was holding the lock, so we've initialized a new head node that is ready to block
        // new readers and writers
        return Futures.of(created.node.writeLockPromiseToken);
      }

      readHead: while (true) {
        final Node head = foundLock.getWriteNode();
        final int previousState = head.internalTerminate();
        if (isBeingRemoved(previousState)) {
          Thread.yield();
          continue findHead;
        } else if (head.shouldRejectTerminator(previousState)) {
          // this node has been write-locked by someone else, we can't acquire it ourselves.
          // re-read head to find the new node created by the write-lock acquirer
          continue readHead;
        } else {
          // successfully set lock bit on this node, need to replace head with successor
          foundLock.setWriteNode(head.linkNext(foundLock.createNode(1)));

          return head.writeLockPromiseToken;
        }
      }
    }
  }

  @Override
  public Optional<ReadLockToken> tryReadLock(final T name) {
    final NodeBox created = new NodeBox();
    final AbstractLinkedEpochReadWriteLock foundLock = computeIfAbsentReader(name, created);

    if (foundLock == created.lock) {
      // no node existed in the map, so our computed node was inserted
      return Optional.of(created.node);
    } else {
      final Node h = foundLock.getReadNode();
      return h.tryReadLock() ? Optional.of(h) : Optional.empty();
    }
  }

  @Override
  public Optional<WriteLockToken> tryWriteLock(final T name) {
    final NodeBox created = new NodeBox();
    final AbstractLinkedEpochReadWriteLock foundLock = computeIfAbsentWriter(name, created);

    if (foundLock == created.lock) {
      return Optional.of(created.node.writeLockPromiseToken);
    } else {
      // the map entry already exists, so someone holds either a reader or writer lock
      return Optional.empty();
    }
  }

  private AbstractLinkedEpochReadWriteLock computeIfAbsentReader(final T name,
      final NodeBox box) {
    return this.map.computeIfAbsent(name, keyName -> {
      final AbstractLinkedEpochReadWriteLock lock = createReaderAcquiredLock(keyName);
      box.lock = lock;
      box.node = lock.getWriteNode();
      return lock;
    });
  }

  private AbstractLinkedEpochReadWriteLock computeIfAbsentWriter(final T name,
      final NodeBox box) {
    return this.map.computeIfAbsent(name, keyName -> {
      final AbstractLinkedEpochReadWriteLock lock = createWriterAcquiredLock(keyName);
      box.lock = lock;
      box.node = lock.getWriteNode().prev;
      return lock;
    });
  }

  boolean isEmpty() {
    return this.map.isEmpty();
  }

  private static final class NodeBox {
    Node node = null;
    AbstractLinkedEpochReadWriteLock lock = null;
  }
}
