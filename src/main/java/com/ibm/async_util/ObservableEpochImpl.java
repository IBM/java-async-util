package com.ibm.async_util;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


/**
 * @see ObservableEpoch
 */
@SuppressWarnings("serial")
class ObservableEpochImpl extends AbstractSimpleEpoch implements ObservableEpoch {
  private final CompletableFuture<Boolean> future = new CompletableFuture<>();

  @Override
  public Optional<EpochToken> enter() {
    return internalEnter() == Status.NORMAL
        ? Optional.of(this)
        : Optional.empty();
  }

  @Override
  public CompletionStage<Boolean> terminate() {
    return internalTerminate() == Status.NORMAL
        ? this.future
        : this.future.thenApply(ignore -> false);
  }

  @Override
  void onCleared() {
    this.future.complete(true);
  }

  @Override
  public CompletionStage<Void> awaitCompletion() {
    return FutureSupport.voided(this.future);
  }
}


