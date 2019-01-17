/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import java.util.Optional;

import org.cleversafe.util.functional.generated.Functions;

/**
 * @see ObservableEpoch
 */
class ObservableEpochImpl extends AbstractSimpleEpoch implements ObservableEpoch {
  private final Promise<Boolean> future = new Promise<>();

  @Override
  public Optional<EpochToken> enter() {
    return isTerminated(internalEnter())
        ? Optional.empty()
        : Optional.of(this);
  }

  @Override
  public Future<Boolean> terminate() {
    return isTerminated(internalTerminate())
        ? this.future.map(Functions.constant(false))
        : this.future;
  }

  @Override
  void onCleared() {
    this.future.accept(true);
  }

  @Override
  public Future<Void> awaitCompletion() {
    return Futures.voided(this.future);
  }
}


