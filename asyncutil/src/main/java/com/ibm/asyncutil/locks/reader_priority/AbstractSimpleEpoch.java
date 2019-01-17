/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.cleversafe.util.async.ObservableEpoch.EpochToken;

/**
 * A base epoch implementation used to construct various epoch mechanisms, e.g. {@link ObservableEpoch}
 */
abstract class AbstractSimpleEpoch implements EpochToken {
  /*
   * Epoch state is maintained by an atomically adjusted integer. Termination sets the high bit of the state; any
   * participants attempting to enter will fail if they find the high bit is set. Otherwise, successful entrants
   * increment the state, thus maintaining a participant count in the remaining bits. On exiting the epoch, participants
   * then decrement this count. If a decrement ever finds that the new count is zero with the terminated bit set, the
   * whenCleared method is run. Similarly, if the terminating CAS finds that the count was previously zero, the
   * whenCleared method is called directly.
   * 
   * This implementation serves as its own epoch token as all operations are performed on the same state integer.
   */
  private static final AtomicIntegerFieldUpdater<AbstractSimpleEpoch> UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(AbstractSimpleEpoch.class, "state");
  static final int TERMINATED = 0x80000000;
  static final int ENTRANT_MASK = ~TERMINATED;
  static final int MAX_COUNT = ENTRANT_MASK;

  /**
   * Checks whether the given epoch state has had its terminated bit set
   */
  static boolean isTerminated(final int epochState) {
    return (epochState & TERMINATED) != 0;
  }

  /**
   * Checks whether the given state has had its terminated bit set and there are no entrants present
   */
  static boolean isCleared(final int epochState) {
    return epochState == TERMINATED;
  }

  /**
   * Extract the count of present entrants from the given state
   */
  static int entrantCount(final int epochState) {
    return epochState & ENTRANT_MASK;
  }

  private volatile int state;

  /**
   * Initialize a new epoch with a state of zero
   */
  public AbstractSimpleEpoch() {
  }

  final boolean compareAndSet(final int expect, final int update) {
    return UPDATER.compareAndSet(this, expect, update);
  }

  /**
   * Initialize a new epoch with a pre-defined state.
   * 
   * @param initialState The initial state this epoch will hold
   * @param weakSet Whether to set the initial state with a plain write (if {@code true}) or with volatile semantics (if
   *        {@code false})
   */
  AbstractSimpleEpoch(final int initialState, final boolean weakSet) {
    if (weakSet) {
      // TODO(java11) use varhandles to write plain (instead of lazySet)
      UPDATER.lazySet(this, initialState);
    } else {
      this.state = initialState;
    }
  }

  /**
   * Method to call when an epoch has been terminated and all participants have exited. May be called from the last
   * exiting participant's thread, or the terminating thread if there are no active participants
   * <p>
   * This method must be overridden to define the epoch's terminal operation
   */
  abstract void onCleared();

  /**
   * Method to call when an entrant has {@link #close() exited} the epoch. The given argument is the state of the epoch
   * after this exit. (Note this differs from {@link #shouldRejectEntrant(int)} and {@link #shouldRejectTerminator(int)}
   * which accept the state prior to its update)
   * <p>
   * By default, this method checks whether the new state {@link #isCleared(int)}, and invokes {@link #onCleared()} if
   * so. This method may be overridden to perform additional operations when existing the epoch -- but remember to also
   * invoke {@link #onCleared()} if overriding this method.
   */
  void onExit(final int newState) {
    if (isCleared(newState)) {
      onCleared();
    }
  }

  /**
   * Method to call before terminators attempt to terminate the epoch. The given argument is the state of the epoch
   * before that termination is counted. If the returned boolean is {@code true}, the termination attempt will not take
   * effect and the epoch state will be unchanged
   * <p>
   * By default, this checks whether the current state {@link #isTerminated(int)}. This method may be overridden to
   * reject terminations when the current state meets some criteria
   */
  boolean shouldRejectTerminator(final int currentState) {
    return isTerminated(currentState);
  }

  /**
   * Method to call before every entrant enters the epoch. The given argument is the state of the epoch before this
   * entrant is counted. If the returned boolean is {@code true}, the entrant will not enter the epoch and the state
   * will be unchanged
   * <p>
   * By default, this checks whether the current state {@link #isTerminated(int)}. This method may be overridden to
   * reject new entrants when the current state meets some criteria
   */
  boolean shouldRejectEntrant(final int currentState) {
    return isTerminated(currentState);
  }

  /**
   * @return the value of the epoch <i>before</i> the enter was performed.
   */
  final int internalEnter() {
    int state;
    do {
      state = this.state;

      if (shouldRejectEntrant(state)) {
        return state;
      }

      if (entrantCount(state) == MAX_COUNT) {
        throw new IllegalStateException("maximum epoch entrants exceeded");
      }

    } while (!compareAndSet(state, state + 1));

    return state;
  }

  /**
   * @return the value of the epoch <i>before</i> the termination was performed.
   */
  final int internalTerminate() {
    int state, newState;
    do {
      state = this.state;

      if (shouldRejectTerminator(state)) {
        // somebody else claimed the termination
        return state;
      }

    } while (!compareAndSet(state, newState = (state | TERMINATED)));

    if (isCleared(newState)) {
      // there weren't any active participants
      onCleared();
    }

    return state;
  }

  public final boolean isTerminated() {
    return isTerminated(this.state);
  }

  final boolean isCleared() {
    return isCleared(this.state);
  }

  @Override
  public final void close() {
    int state, newState;
    do {
      state = this.state;

      if (entrantCount(state) == 0) {
        throw new IllegalStateException("excessively closed epoch");
      }

    } while (!compareAndSet(state, newState = (state - 1)));

    onExit(newState);
  }

  @Override
  public String toString() {
    final int state = this.state;
    return "Epoch [isTerminated=" + isTerminated(state)
        + ", entrants=" + entrantCount(state)
        + "]";
  }
}
