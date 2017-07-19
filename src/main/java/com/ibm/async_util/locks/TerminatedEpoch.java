package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import com.ibm.async_util.util.StageSupport;

/**
 * @see ObservableEpoch#newTerminatedEpoch()
 */
class TerminatedEpoch implements ObservableEpoch {
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
