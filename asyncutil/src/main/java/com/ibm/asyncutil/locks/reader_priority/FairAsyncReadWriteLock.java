/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

/**
 * An implementation of the {@link AsyncReadWriteLock} interface which enforces reader/writer fairness -- i.e. new
 * readers will queue behind waiting writers
 */
public class FairAsyncReadWriteLock extends AbstractLinkedEpochReadWriteLock {

  static class FairNode extends Node {
    /*
     * This node implementation uses the default epoch behavior of prohibiting new entrants once an epoch is terminated.
     * This enforces the fairness behavior of the lock, preventing new readers from entering a given node once a writer
     * has entered it.
     */

    FairNode(final int initialEntrants) {
      super(initialEntrants);
    }
  }

  public FairAsyncReadWriteLock() {
    super();
  }

  /**
   * @see AbstractLinkedEpochReadWriteLock#AbstractLinkedEpochReadWriteLock(boolean)
   */
  FairAsyncReadWriteLock(final boolean signatureDisambiguatingParameter) {
    super(signatureDisambiguatingParameter);
  }

  @Override
  final Node getReadNode() {
    // the fair implementation acquires both read and write locks on the same node, because they have equal priority
    return getWriteNode();
  }

  @Override
  Node createNode(final int initialState) {
    return new FairNode(initialState);
  }
}
