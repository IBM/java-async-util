package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.ibm.async_util.util.StageSupport;


/**
 * @see ObservableEpoch
 */
@SuppressWarnings("serial")
class ObservableEpochImpl extends AbstractSimpleEpoch implements ObservableEpoch {
  /**
   *
   */
  private static final long serialVersionUID = -3263030267313569994L;
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
    return StageSupport.voided(this.future);
  }
}


