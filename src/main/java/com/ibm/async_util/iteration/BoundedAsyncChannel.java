/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.iteration;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A version of {@link AsyncChannel} that provides a mechanism for backpressure.
 *
 * <p>
 * The documentation from {@link AsyncChannel} largely applies here. Backpressure refers to the
 * signal sent to senders that the channel is "full" and the sender should stop sending values for
 * some period of time. Typically a channel becomes "full" because values are being sent into the
 * channel faster than the consumer is capable of consuming them. Without backpressure, the senders
 * could cause an out of memory condition if they eventually sent too many messages into the
 * channel. Users are expected to respect backpressure by refraining from making a subsequent call
 * to {@link #send} until the {@link CompletionStage} returned by the previous call completes.
 *
 * <p>
 * Currently you can produce a bounded channel with {@link AsyncChannels#bounded()} or
 * {@link AsyncChannels#buffered(int)}.
 *
 * <p>
 * Consider this example implemented without backpressure
 *
 * <pre>
 * {@code
 * AsyncIterator<Integer> produce() {
 *   AsyncChannel channel = AsyncChannels.unbounded();
 *   pool.submit(() -> {
 *     while (keepGoing) {
 *       channel.send(i++);
 *     }
 *     channel.terminate();
 *   });
 * }
 * produce().forEach(i -> {
 *   slowWriteToDisk(i);
 * });
 * }
 * </pre>
 *
 * Because generating work is a cheap in-memory operation but consuming it is a slow IO operation,
 * the sender will dramatically outpace the consumer in this case. Soon, the process will run out of
 * memory, as the sender continues to queue ints for the consumer to write. Instead we can use a
 * bounded channel:
 *
 * <pre>
 * {@code
 * AsyncIterator<Integer> produce() {
 *   final AsyncChannel<Integer> channel = AsyncChannels.bounded();
 *
 *   //blocking sends on pool
 *   pool.submit(() -> {
 *     while (shouldContinue()) {
 *       channel.send(i++).toCompletableFuture().join();
 *     }
 *     channel.terminate();
 *   });
 *   return channel;
 *}
 *
 *   // consumer doesn't know or care channel is bounded
 * produce().forEach(i -> {
 *   slowWriteToDisk(i);
 * });
 *}
 * </pre>
 *
 * Senders of course can be implemented without blocking while still respecting backpressure:
 *
 * <pre>
 * {@code
 * AsyncIterator<Integer> produce() {
 *   final AsyncChannel<Integer> channel = AsyncChannels.bounded();
 *
 *   // alternative approach to sending: async sender
 *   AsyncIterators
 *       .iterate(i -> i + 1)
 *       // send to channel
 *       .thenApply(i -> channel.send(i))
 *       // consumes futures one by one
 *       .takeWhile(ig -> shouldContinue())
 *       .consume()
 *       // finished, terminate channel
 *       .thenRun(() -> channel.terminate());
 *
 *   return channel;
 *}
 *}
 * </pre>
 *
 * <p>
 * An important point is that trying to send is the only way to be notified that the queue is full.
 * In practice, this means that if your number of senders is very large you can still consume too
 * much memory even if you are respecting the send interface.
 *
 * @param <T> the type of the items sent and consumed from this channel
 * @see AsyncIterator
 * @see AsyncChannel
 * @see AsyncChannels
 */
public interface BoundedAsyncChannel<T> extends AsyncIterator<T> {

  /**
   * Send a value into this channel that can be consumed via the {@link AsyncIterator} interface.
   *
   * <p>
   * This method is thread safe - multiple threads can send values into this channel concurrently.
   * This channel is bounded, so after a call to {@code send} a {@link CompletionStage} is returned
   * to the sender. When the stage finishes, consumption has progressed enough that the channel is
   * again willing to accept messages. The implementation decides when a channel is writable: it
   * could require that all outstanding values are consumed by the consumer, it could allow a
   * certain number of values to be buffered before applying back pressure, or it could use some
   * out-of-band metric to decide.
   *
   * @param item element to send into the channel
   * @return a {@link CompletionStage} that completes when the channel is ready to accept another
   *         message. It completes with true if the item was accepted, false if it was rejected
   *         because the channel has already been terminated.
   * @see AsyncChannel#send
   */
  CompletionStage<Boolean> send(T item);

  /**
   * Terminate the channel. After termination subsequent attempts to {@link #send} into the channel
   * will fail.
   *
   * <p>
   * After the channel is terminated, all subsequent sends will return stages that will complete
   * with false. After the consumer consumes whatever was sent before the terminate, the consumer
   * will receive an {@link AsyncIterator.End} marker. When the {@link CompletionStage} returned by
   * this method completes, no more messages will ever make it into the channel. Equivalently, all
   * stages generated by {@link #send} that will complete with {@code true} will have been completed
   * by the time the returned stage completes.
   *
   * @return a {@link CompletionStage} that indicates when all sends that were sent before the
   *         terminate have made it into the channel
   * @see AsyncChannel#terminate()
   */
  CompletionStage<Void> terminate();

  /**
   * Gets a result from the channel if one is immediately available.
   *
   * <p>
   * This method consumes parts of the channel, so like the consumption methods on
   * {@link AsyncIterator}, this method is not thread-safe and should be used in a single threaded
   * fashion. After {@link #terminate()} is called and all outstanding results are consumed, poll
   * will always return empty. This method <b> should not </b> be used if there are null values in
   * the channel. <br>
   * Notice that the channel being closed is indistinguishable from the channel being transiently
   * empty. To discover that no more results will ever be available, you must use the normal means
   * on {@link AsyncIterator}: either calling {@link #nextFuture()} and seeing if the result
   * indicates an end of iteration when the future completes, or using one of the consumer methods
   * that only complete once the channel has been closed.
   *
   * @throws NullPointerException if the polled result is null
   * @return a present T value if there was one immediately available in the channel, empty if the
   *         channel is currently empty
   * @see AsyncChannel#poll()
   */
  Optional<T> poll();
}
