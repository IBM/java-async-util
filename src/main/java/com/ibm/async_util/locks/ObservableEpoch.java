package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A concurrency mechanism which maintains a period of activity -- an epoch --
 * during which participants may successfully enter it, that later can be
 * terminated. After an epoch is terminated, new entrants are rejected from
 * entering but the terminator waits for any remaining participants to exit
 */
public interface ObservableEpoch {

	/**
	 * Attempt to secure a position in the active epoch, failing if it has
	 * already been terminated.
	 * 
	 * @return an {@link Optional} populated with an {@link EpochToken}
	 *         associated with the active epoch if it has not been terminated.
	 *         Otherwise, returns an empty Optional
	 */
	Optional<EpochToken> enter();

	/**
	 * Atomically ends the active epoch, preventing new entrants from
	 * successfully entering and returning a {@link CompletionStage} that triggers once
	 * allOf active participants have exited.
	 * 
	 * @return a {@link CompletionStage}, which will complete after the last open
	 *         {@link EpochToken} has been closed. The value of the future will
	 *         be {@code false} if this epoch was already terminated, otherwise
	 *         {@code true} for the single call that terminates this epoch
	 */
	CompletionStage<Boolean> terminate();

	/**
	 * Returns {@code true} if this epoch has been terminated. This boolean does
	 * <i>not</i> indicate whether allOf active participants have exited the
	 * epoch, only whether the {@link #terminate()} method has been called and
	 * subsequent entrants will be rejected
	 * 
	 * @return true iff this epoch has been terminated
	 */
	boolean isTerminated();

	/**
	 * Waits for the epoch to complete, returning a future that triggers after
	 * the epoch has been {@link #terminate() terminated} and allOf participants
	 * have exited. Note that this method does <i>not</i> terminate the epoch
	 * itself -- new entrants may enter and exit freely after this method is
	 * called, and a separate call to {@link #terminate()} must be made before
	 * this returned future completes
	 * 
	 * @return a {@link CompletionStage} which will complete after a call to
	 *         {@link #terminate()} has been made, and the last open
	 *         {@link EpochToken} has been closed.
	 */
	CompletionStage<Void> awaitCompletion();

	/**
	 * @return a new {@link ObservableEpoch} instance
	 */
	static ObservableEpoch newEpoch() {
		// TODO implement hierarchical back-off epoch
		return new ObservableEpochImpl();
	}

	/**
	 * @return a new {@link ObservableEpoch} instance which is already
	 *         terminated. This is useful for initializing an epoch to something
	 *         that isn't null, but won't allow anyone to enter it.
	 */
	static ObservableEpoch newTerminatedEpoch() {
		return TerminatedEpoch.INSTANCE;
	}

	/**
	 * A token signifying successful entry in the active epoch. This token must
	 * be {@link EpochToken#close() closed} when its work has completed in order
	 * to exit the epoch.
	 * 
	 * @see ObservableEpoch
	 */
	public interface EpochToken extends AutoCloseable {
		/**
		 * Exits the epoch that was previously entered.
		 */
		@Override
		void close();
	}
}
