/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.cleversafe.util.PlatformDependent;

/**
 * An {@link AsyncReadWriteLock} implementation which favors readers over writers. Read lock acquisitions may barge
 * ahead of already-waiting write lock acquisitions, placing themselves in a higher position in the waiting queue or
 * acquiring the read lock if it's currently available. Write lock acquisitions amongst themselves maintain "fair" FIFO
 * ordering.
 * <p>
 * This reader priority acquisition can inherently lead to writer starvation: if reader acquisitions continuously
 * overlap, writers will never make progress. Users of this lock must ensure that their acquisitions have some means of
 * permitting writer progress in order to avoid starvation. Otherwise, consider using the {@link FairAsyncReadWriteLock}
 * which prevents starvation by enforcing fair acquisition ordering.
 * <p>
 * Note that this implementation does not correspond to the standard library's
 * {@link java.util.concurrent.locks.ReentrantReadWriteLock} in unfair mode. The unfair mode's reader and writer
 * acquisition ordering is undefined, while this implementation has a defined ordering for both readers and writers.
 * This implementation also does not imply any throughput benefit over the more conventional
 * {@link FairAsyncReadWriteLock fair alternative}; its purpose is primarily for cases where progress for readers is
 * preferred over writers.
 */
public class ReaderPriorityAsyncReadWriteLock extends AbstractLinkedEpochReadWriteLock {
  /*
   * This lock implementation achieves reader barging by modifying the default epoch behavior to allow entrants after a
   * node has been terminated. So long as the entrant count is non-zero (even on terminated nodes) new readers are
   * allowed to join, but the writer must be released at any point when entrants are zero -- this stipulation resolves
   * races around releasing the epoch and triggering futures, stalling new readers until a new node is inserted. This
   * allows reader favoritism/barging/unfairness for an individual node; to make the lock itself unfair, we introduce an
   * interior node pointer to the oldest node that readers may still join. This interior node follows the most recent
   * write-lock release, where new readers would be allowed to barge ahead of queued writers, as opposed to the head
   * pointer which follows the most recent write-lock acquisition.
   */

  class ReaderPriorityNode extends Node {
    ReaderPriorityNode(final int initialEntrants) {
      super(initialEntrants);
    }

    @Override
    Node linkNext(final Node node) {
      // we must introduce a store fence to ensure the node we link to `next` is fully constructed before publishing
      // see more commentary under `getReadNode`
      if (!PlatformDependent.storeFence()) {
        throw new AssertionError("fences unsupported");
      }
      return super.linkNext(node);
    }

    @Override
    void writerAcquired() {
      ReaderPriorityAsyncReadWriteLock.this.readerPredecessor = this;
    }

    @Override
    boolean shouldRejectEntrant(final int currentState) {
      // new readers are only rejected if the node is both terminated and empty of readers
      return isCleared(currentState);
    }
  }

  /**
   * This is the interior node pointer that tracks the (immediately preceding) node where new readers can try to
   * acquire. This is advanced every time that a writer is triggered (i.e. either acquires from the queue or
   * tryWriteLock) because that signifies that readers can no longer enter the node which just triggered a writer.
   * Concretely, because this pointer precedes the reader node, it's updated to the node which triggered the writer so
   * that readers enter the successor node.
   * <p>
   * It lags the true insertion point by 1 node because there are brief periods of time (after a writer acquisition)
   * during which a valid node might not exist (but a predecessor always does, created in the constructor). Updating
   * this valid node is driven by lock-release code, but new nodes are created by lock-acquisition code, which are
   * largely independent of each other. It may be possible to rewrite the readers to use an up-to-date pointer and spin
   * for a period when the node doesn't exist (indeed, the current implementation spins on this predecessor's `next`
   * ref), but attempts to implement it directly tended to bleed implementation details into the abstract lock class and
   * interfered with otherwise agnostic code.
   */
  private volatile Node readerPredecessor;

  public ReaderPriorityAsyncReadWriteLock() {
    super();
    /*
     * The abstract lock implementation employs node self-linking when removing nodes from the backing queue. As a
     * consequence, given that our interior reader node is a predecessor of the node of interest, there will be times
     * when we attempt to traverse forward and unknowingly encounter the same node, returning it as the successor. This
     * alone is not an issue, because callers of `getReadNode` have encompassing retry mechanisms which will loop back
     * and eventually find an appropriate up-to-date node. However those retry mechanisms rely on inspecting the given
     * node (e.g. attempting to enter the epoch), so we must populate our predecessor with a fully formed node instead
     * of the default constructor's sentinel.
     */
    // create a sentinel which cannot be entered by calls to e.g. `acquireReadLock` and will force re-reading
    @SuppressWarnings("squid:S1699") // calling an overridable method is the point here
    final Node sentinel = createNode(AbstractSimpleEpoch.TERMINATED);
    sentinel.linkNext(getWriteNode());
    this.readerPredecessor = sentinel;
  }

  /**
   * @see AbstractLinkedEpochReadWriteLock#AbstractLinkedEpochReadWriteLock(boolean)
   */
  ReaderPriorityAsyncReadWriteLock(final boolean signatureDisambiguatingParameter) {
    super(signatureDisambiguatingParameter);
  }

  @Override
  final Node getReadNode() {
    // read our interior pointer preceding the read node.
    final Node pred = this.readerPredecessor;
    /*
     * Careful observers will note here that we're performing a plain read of a field potentially updated by another
     * thread, and rightfully feel concerned. If there were a means of accessing a plain field in the parent class using
     * volatile semantics, that would be the ideal solution and save us from black magic (TODO(java11) varhandles!)
     * 
     * To understand the soundness here, consider the possible states of a node's `next` field: (1)
     * default/uninitialized null, or (2) populated with some node after null. Importantly, once the field is non-null,
     * it is never set back to null: subsequent changes are either to nodes farther forward in the queue during the
     * lock-release trampoline, or GC unlinking which sets a node's references to itself.
     * 
     * (1) only occurs a the head of the list, where new nodes are inserted. I.e. when a node is the leading node it
     * will not have a next pointer. Our interior pointer with which we access this `next`, `readerPredecessor`, would
     * always point _forward_ to the node that readers may enter, so it can only point to the head node for a brief
     * period of time while a new node is being constructed and inserted. Specifically, readerPredecessor can only be
     * set to the same node as the head by immediate writer acquisitions (either successful tryWriteLock, or
     * acquireWriteLock where the future is already complete). The key point is that readerPredecessor.next can only be
     * null while readerPredecessor == this.head; after the epoch is terminated and before the new node is linked into
     * the queue and the head is updated.
     * 
     * Should a reader ever find that the next pointer is null (and due to the plain read, this can be a stale view of
     * the field, lagging the "true" state of the lock) then it must check whether the head is equal to the reader
     * predecessor. If so, this is the short time period before a new node is published, and the reader should spin
     * until that publication, signaled by a change in the head node. Because the new node is linked into the queue
     * before the head is updated, the volatile read of the head node will ensure that a valid non-null node is visible
     * after this spin loop -- whether this was a true inter-node period and required a spin wait, or the plain read
     * yielded a stale view of 'null', the new `next` state will be visible after the read of head.
     * 
     * (2) is the other case where the node already points to some non-null `next`. Unfortunately, because this a plain
     * read of a field which can be updated in another thread, there is no enforced ordering before writing to the
     * field, so the published new node may not be fully constructed. To correct this, nodes in this lock implementation
     * introduce store fences to their linking method (see `linkNext`). This ensures that a new node is fully
     * constructed before it can be published via a `next` pointer and subsequently read here. After reading this field,
     * all operations are dependent on the object (e.g. node.internalEnter(), node.tryReadLock()) so further explicit
     * ordering is not necessary.
     * 
     * Next pointers are changed by lock-release code during trampolining, which means that the nodes can change from
     * one non-null to another non-null. Whether this node can be entered is determined by this method's encompassing
     * caller. Because the readerPredecessor is also updated by lock-release, the caller's retry mechanisms will call
     * this method again if necessary, and the reads of readerPredecessor will eventually find a node which can be
     * entered by the reader. `tryReadLock` does not retry, however if the `next` node read from the readerPredecessor
     * cannot be entered, a writer must have been locking so an acquisition failure is consistent for some serialized
     * version of events.
     */
    Node reader = pred.next;

    // check case (1)
    if (reader == null) {
      while (getWriteNode() == pred) {
        // successor doesn't exist yet, spin
      }
      // volatile read of the head(writeNode) ensures a new view of the next link
      reader = pred.next;
      assert reader != null;
    }
    return reader;
  }

  @Override
  Node createNode(final int initialState) {
    return new ReaderPriorityNode(initialState);
  }
}
