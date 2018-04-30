/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.ibm.asyncutil.util.StageSupport;


/**
 * @author Renar Narubin
 * @see AsyncEpoch
 */
@SuppressWarnings("serial")
class AsyncEpochImpl extends AbstractSimpleEpoch implements AsyncEpoch {
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


