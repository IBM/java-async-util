//
// (C) Copyright IBM Corp. 2005 All Rights Reserved.
//
// Contact Information:
//
// IBM Corporation
// Legal Department
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// END-OF-HEADER
//
// -----------------------
// @author: rkhadiwala
//
// Date: Feb 14, 2017
// ---------------------

package com.ibm.async_util.iteration;

import java.util.Optional;

/**
 * An unbounded async multi-producer-single-consumer channel.
 *
 * <p>This class provides a channel abstraction that allows multiple senders to place values into
 * the channel synchronously, and a single consumer to consume values as they become available
 * asynchronously. You can construct an {@link AsyncChannel} with the static methods on {@link
 * AsyncChannels}.
 *
 * <p>This interface represents an <i> unbounded </i> queue, meaning there is no mechanism to notify
 * the sender that the queue is "full" (nor is there a notion of the queue being full to begin
 * with). The channel will continue to accept values as fast as the senders can {@link #send} them,
 * regardless of the rate at which the values are being consumed. If senders produce a lot of values
 * much faster than the consumption rate, it will lead to an out of memory, so users are responsible
 * for enforcing that the channel does not grow too large. If you would like a channel abstraction
 * that provides backpressure, see {@link BoundedAsyncChannel}.
 *
 * <p>This channel can be terminated by someone calling {@link #terminate()}, it can be called by
 * consumers or senders. It is strongly recommended that all instances of this class eventually be
 * terminated. Mose terminal operations on {@link AsyncIterator} return {@link
 * java.util.concurrent.CompletionStage CompletionStages} that whose stage will not complete until
 * the channel is terminated. After the channel is terminated, subsequent {@link #send}s are
 * rejected, though consumers of the channel will still receive any values that were sent before the
 * termination.
 *
 * <p>Typically you'll want to use a channel when you have some "source" of items, and want to
 * consume them asynchronously as the become available. Some examples of sources could be a
 * collection of {@link java.util.concurrent.CompletionStage CompletionStages}, bytes off of a
 * socket, results produced by dedicated worker threads, etc. Suppose you had scenario where you had
 * many threads doing some CPU intensive computation, and you'd send their answers off to some
 * server somewhere one at a time.
 *
 * <pre>{@code
 * AsyncChannel<Integer> channel = AsyncChannels.unbounded();
 * for (i = 0; i < numThreads; i++) {
 *   // spawn threads that send results to channel
 *   threadpool.submit(() -> {
 *      while (canStillCompute) {
 *        int num = computeReallyExpensiveThing();
 *        channel.send(num);
 *      }
 *    });
 * }
 *
 * //consumer of channel, sending numbers to a server one at a time
 * channel
 *   // lazily map numbers to send
 *   .thenCompose(number -> sendToServer(number))
 *   // consume all values
 *   .consume()
 *   // iteration stopped (meaning channel was terminated)
 *   .thenAccept(ig -> sendToServer("no more numbers!");
 *
 * threadpool.awaitTermination();
 * // terminate the channel, done computing
 * channel.terminate();
 *
 * }</pre>
 *
 * <p>It is also convenient to use a channel to merge many {@link AsyncIterator}s together. Think if
 * we were the destination server in the previous example, and we had many compute servers sending
 * us numbers they were computing. If we used {@link AsyncIterator#concat} in the following example,
 * we would wait until we got all the work from the first iterator to move onto the next. With a
 * channel we process each number as soon as it becomes available.
 *
 * <pre>{@code
 * AsyncIterator<Integer> getNumbersFrom(ServerLocation ip);
 * AsyncChannel channel = AsyncChannels.unbounded();
 * futures = ips.stream()
 *
 *   // get an AsyncIterator of numbers from each server
 *  .map(this::getNumbersFrom)
 *
 *   // send each number on each iterator into the channel as they arrive
 *  .forEach(asyncIterator -> asyncIterator.forEach(t -> channel.send(t)))
 *
 *  // bundle futures into a list
 *  .collect(Collectors.toList());
 *
 *  // terminate the channel whenever we're done sending
 *  Combinators.all(futures).thenAccept(ignore -> channel.terminate());
 *
 *  // prints each number returned by servers as they arrive
 *  channel
 *    .forEach(num -> System.out.println(num))
 *    .thenAccept(ig -> System.out.println("finished getting all numbers")));
 * }</pre>
 *
 * <p>A reminder, all topics addressed in the documentation of {@link AsyncIterator} apply to this
 * interface as well. Most importantly this means:
 *
 * <ul>
 *   <li>Consumption of an AsyncIterator is <b> not </b> thread safe
 *   <li>Lazy methods on AsyncIterator like map/flatMap don't consume anything. Make sure you
 *       actually use a consumption operation somewhere, otherwise no one will ever read what was
 *       sent
 * </ul>
 *
 * @param <T> The type of the elements in this channel
 * @see AsyncChannels
 * @see BoundedAsyncChannel
 */
public interface AsyncChannel<T> extends AsyncIterator<T> {
  /**
   * Sends a value into this channel that can be consumed via the {@link AsyncIterator} interface.
   *
   * <p>This method is thread safe - multiple threads can send values into this channel
   * concurrently. This channel is unbounded, so it will continue accept new items immediately and
   * store them in memory until they can be consumed. If you are sending work faster than you can
   * consume it, this can easily lead to an out of memory condition.
   *
   * @param item the item to be sent into the channel
   * @return true if the item was accepted, false if it was rejected because the channel has already
   *     been terminated
   */
  boolean send(T item);

  /**
   * Terminates the channel, disabling {@link #send}.
   *
   * <p>After the channel is terminated all subsequent sends will be rejected, returning false.
   * After the consumer consumes whatever was sent before the terminate, the consumer will receive
   * an end of iteration notification.
   *
   * <p>This method is thread-safe, and can be called multiple times. An attempt to terminate after
   * termination has already occurred is a no-op.
   */
  void terminate();

  /**
   * Gets a result from the channel if there is one ready right now.
   *
   * <p>This method consumes parts of the channel, so like the consumption methods on {@link
   * AsyncIterator}, this method is not thread-safe should be used in a single threaded fashion.
   * After {@link #terminate()} is called and all outstanding results are consumed, poll will always
   * return empty. This method <b> should not </b> be used if there are null values in the channel.
   * <br>
   * Notice that the channel being closed is indistinguishable from the channel being transiently
   * empty. To discover that no more results will ever be available, you must use the normal means
   * on {@link AsyncIterator}: either calling {@link #nextFuture()} and seeing if the result
   * indicates an end of iteration when the future completes, or using one of the consumer methods
   * that only complete once the channel has been closed.
   *
   * @throws NullPointerException if the polled result is null
   * @return a present T value if there was one immediately available in the channel, empty if the
   *     channel is currently empty
   */
  Optional<T> poll();
}
