package com.ibm.async_util.locks;

import com.ibm.async_util.util.FutureSupport;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
    return CompletableFuture.completedFuture(false);
  }

  @Override
  public boolean isTerminated() {
    return true;
  }

  @Override
  public CompletionStage<Void> awaitCompletion() {
    return FutureSupport.voidFuture();
  }
}
