package com.ibm.async_util.locks;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A base epoch implementation used to construct various epoch mechanisms, e.g.
 * {@link ObservableEpoch}
 */
@SuppressWarnings("serial")
abstract class AbstractSimpleEpoch
		// opportunistically extend AtomicInteger for state
		extends AtomicInteger implements ObservableEpoch.EpochToken {
	/*
	 * Epoch state is maintained by an atomically adjusted integer. Termination
	 * sets the high bit of the state; any participants attempting to enter will
	 * fail if they find the high bit is set. Otherwise, successful entrants
	 * increment the state, thus maintaining a participant count in the
	 * remaining bits. On exiting the epoch, participants then decrement this
	 * count. If a decrement ever finds that the new count is zero with the
	 * terminated bit set, the whenCleared method is run. Similarly, if the
	 * terminating CAS finds that the count was previously zero, the whenCleared
	 * method is called directly.
	 *
	 * This implementation serves as its own epoch token as all operations are
	 * performed on the same state integer.
	 */
	static final int TERMINATED = 0x80000000;
	static final int ENTRANT_MASK = ~TERMINATED;
	static final int MAX_COUNT = ENTRANT_MASK;

	/**
	 * Status codes used to differentiate possible return values of
	 * {@link #internalEnter} and {@link #internalTerminate}
	 */
	enum Status {
		NORMAL, TERMINATED, SPECIAL
	}

	/**
	 * Method to call when an epoch has been terminated and all participants
	 * have exited. May be called from the last exiting participant's thread, or
	 * the terminating thread if there are no active participants
	 * <p>
	 * This method must be overridden to define the epoch's terminal operation
	 */
	abstract void onCleared();

	/**
	 * Method to call when an entrant has {@link #close() exited} the epoch. The
	 * given argument is the state of the epoch after this exit; masked with
	 * ENTRANT_MASK this will yield the current count of entrants.
	 * <p>
	 * This method may be overridden to inject some operation after each exit
	 */
	void onExit(final int state) {
	}

	/**
	 * Method to call before terminators attempt to terminate the epoch. The
	 * given argument is the state of the epoch before that termination is
	 * counted. If the returned boolean is {@code true}, the termination attempt
	 * will not take effect and {@link Status#SPECIAL} will be returned by that
	 * call to {@link #internalTerminate()}
	 * <p>
	 * This method may be overridden to reject terminations when the current
	 * state meets some criteria
	 */
	boolean isSpecialTerminateState(final int state) {
		return false;
	}

	/**
	 * Method to call before every entrant enters the epoch. The given argument
	 * is the state of the epoch before this entrant is counted. If the returned
	 * boolean is {@code true}, the entrant will not enter the epoch and
	 * {@link Status#SPECIAL} will be returned by that call to
	 * {@link #internalEnter()}
	 * <p>
	 * This method may be overridden to reject new entrants when the current
	 * state meets some criteria
	 */
	boolean isSpecialEnterState(final int state) {
		return false;
	}

	final Status internalEnter() {
		int state;
		do {
			state = get();

			if (isSpecialEnterState(state)) {
				return Status.SPECIAL;
			}

			if ((state & TERMINATED) != 0) {
				return Status.TERMINATED;
			}

			if (state == MAX_COUNT) {
				throw new IllegalStateException("maximum epoch entrants exceeded");
			}

		} while (!compareAndSet(state, state + 1));

		return Status.NORMAL;
	}

	/**
	 * @return {@link Status#SPECIAL} if the current state before this call
	 *         meets the {@link #isSpecialTerminateState(int)} criteria.
	 *         Otherwise returns {@link Status#TERMINATED} if the epoch was
	 *         <i>already</i> terminated (not by this call). Otherwise returns
	 *         {@link Status#NORMAL} (i.e. uniquely terminated by this call)
	 */
	final Status internalTerminate() {
		int state, newState;
		do {
			state = get();

			if (isSpecialTerminateState(state)) {
				return Status.SPECIAL;
			}

			if ((state & TERMINATED) != 0) {
				// somebody else claimed the termination
				return Status.TERMINATED;
			}

		} while (!compareAndSet(state, newState = (state | TERMINATED)));

		if (newState == TERMINATED) {
			// there weren't any active participants
			onCleared();
		}

		return Status.NORMAL;
	}

	public final boolean isTerminated() {
		return (get() & TERMINATED) != 0;
	}

	final boolean isCleared() {
		return get() == TERMINATED;
	}

	@Override
	public final void close() {
		int state, newState;
		do {
			state = get();

			if ((state & ENTRANT_MASK) == 0) {
				throw new IllegalStateException("excessively closed epoch");
			}

		} while (!compareAndSet(state, newState = (state - 1)));

		onExit(newState);

		if (newState == TERMINATED) {
			onCleared();
		}
	}

	@Override
	public String toString() {
		final int state = get();
		return "Epoch [isTerminated=" + ((state & TERMINATED) != 0) + ", entrants=" + (state & ENTRANT_MASK) + "]";
	}
}
