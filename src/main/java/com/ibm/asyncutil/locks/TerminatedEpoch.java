/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import com.ibm.asyncutil.util.StageSupport;

/**
 * @see AsyncEpoch#newTerminatedEpoch()
 */
class TerminatedEpoch implements AsyncEpoch {
  static final TerminatedEpoch INSTANCE = new TerminatedEpoch();

  private TerminatedEpoch() {}

  @Override
  public Optional<EpochToken> enter() {
    return Optional.empty();
  }

  @Override
  public CompletionStage<Boolean> terminate() {
    return StageSupport.completedStage(false);
  }

  @Override
  public boolean isTerminated() {
    return true;
  }

  @Override
  public CompletionStage<Void> awaitCompletion() {
    return StageSupport.voidFuture();
  }
}
