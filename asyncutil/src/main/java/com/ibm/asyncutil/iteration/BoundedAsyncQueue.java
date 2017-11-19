/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.iteration;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A version of {@link AsyncQueue} that provides a mechanism for backpressure.
 *
 * <p>
 * The documentation from {@link AsyncQueue} largely applies here. Backpressure refers to the signal
 * sent to senders that the queue is "full" and the sender should stop sending values for some
 * period of time. Typically a queue becomes "full" because values are being sent into the queue
 * faster than the consumer is capable of consuming them. Without backpressure, the senders could
 * cause an out of memory condition if they eventually sent too many messages into the queue. Users
 * are expected to respect backpressure by refraining from making a subsequent call to {@link #send}
 * until the {@link CompletionStage} returned by the previous call completes.
 *
 * <p>
 * Currently you can produce a bounded queue with {@link AsyncQueues#bounded()} or
 * {@link AsyncQueues#buffered(int)}.
 *
 * <p>
 * Consider this example implemented without backpressure
 *
 * <pre>
 * {@code
 * AsyncIterator<Integer> produce() {
 *   AsyncQueue queue = AsyncQueues.unbounded();
 *   pool.submit(() -> {
 *     while (keepGoing) {
 *       queue.send(i++);
 *     }
 *     queue.terminate();
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
 * bounded queue:
 *
 * <pre>
 * {@code
 * AsyncIterator<Integer> produce() {
 *   final AsyncQueue<Integer> queue = AsyncQueues.bounded();
 *
 *   //blocking sends on pool
 *   pool.submit(() -> {
 *     while (shouldContinue()) {
 *       queue.send(i++).toCompletableFuture().join();
 *     }
 *     queue.terminate();
 *   });
 *   return queue;
 *}
 *
 *   // consumer doesn't know or care queue is bounded
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
 *   final AsyncQueue<Integer> queue = AsyncQueues.bounded();
 *
 *   // alternative approach to sending: async sender
 *   AsyncIterators
 *       .iterate(i -> i + 1)
 *       // send to queue
 *       .thenApply(i -> queue.send(i))
 *       // consumes futures one by one
 *       .takeWhile(ig -> shouldContinue())
 *       .consume()
 *       // finished, terminate queue
 *       .thenRun(() -> queue.terminate());
 *
 *   return queue;
 *}
 *}
 * </pre>
 *
 * <p>
 * An important point is that trying to send is the only way to be notified that the queue is full.
 * In practice, this means that if your number of senders is very large you can still consume too
 * much memory even if you are respecting the send interface.
 *
 * @param <T> the type of the items sent and consumed from this queue
 * @see AsyncIterator
 * @see AsyncQueue
 * @see AsyncQueues
 */
public interface BoundedAsyncQueue<T> extends AsyncIterator<T> {

  /**
   * Sends a value into this queue that can be consumed via the {@link AsyncIterator} interface.
   *
   * <p>
   * This method is thread safe - multiple threads can send values into this queue concurrently.
   * This queue is bounded, so after a call to {@code send} a {@link CompletionStage} is returned to
   * the sender. When the stage finishes, consumption has progressed enough that the queue is again
   * willing to accept messages. The implementation decides when a queue is writable: it could
   * require that all outstanding values are consumed by the consumer, it could allow a certain
   * number of values to be buffered before applying back pressure, or it could use some out-of-band
   * metric to decide.
   *
   * @param item element to send into the queue
   * @return a {@link CompletionStage} that completes when the queue is ready to accept another
   *         message. It completes with true if the item was accepted, false if it was rejected
   *         because the queue has already been terminated.
   * @see AsyncQueue#send
   */
  CompletionStage<Boolean> send(T item);

  /**
   * Terminates the queue. After termination subsequent attempts to {@link #send} into the queue will
   * fail.
   *
   * <p>
   * After the queue is terminated, all subsequent sends will return stages that will complete with
   * false. After the consumer consumes whatever was sent before the terminate, the consumer will
   * receive an {@link AsyncIterator.End} marker. When the {@link CompletionStage} returned by this
   * method completes, no more messages will ever make it into the queue. Equivalently, all stages
   * generated by {@link #send} that will complete with {@code true} will have been completed by the
   * time the returned stage completes.
   *
   * @return a {@link CompletionStage} that indicates when all sends that were sent before the
   *         terminate have made it into the queue
   * @see AsyncQueue#terminate()
   */
  CompletionStage<Void> terminate();

  /**
   * Gets a result from the queue if one is immediately available.
   *
   * <p>
   * This method consumes parts of the queue, so like the consumption methods on
   * {@link AsyncIterator}, this method is not thread-safe and should be used in a single threaded
   * fashion. After {@link #terminate()} is called and all outstanding results are consumed, poll
   * will always return empty. This method <b> should not </b> be used if there are null values in
   * the queue. <br>
   * Notice that the queue being closed is indistinguishable from the queue being transiently empty.
   * To discover that no more results will ever be available, you must use the normal means on
   * {@link AsyncIterator}: either calling {@link #nextStage()} and seeing if the result indicates
   * an end of iteration when the stage completes, or using one of the consumer methods that only
   * complete once the queue has been closed.
   *
   * @throws NullPointerException if the polled result is null
   * @return a present T value if there was one immediately available in the queue, empty if the
   *         queue is currently empty
   * @see AsyncQueue#poll()
   */
  Optional<T> poll();
}
