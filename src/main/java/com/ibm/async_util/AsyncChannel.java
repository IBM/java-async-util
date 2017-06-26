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

package com.ibm.async_util;

import java.util.Optional;

/**
 * An unbounded async multi-producer-single-consumer channel.
 * 
 * This class provides a channel abstraction that allows multiple senders to place values into the
 * channel synchronously, and a single consumer to consume values as they become available
 * asynchronously. You can construct an {@link AsyncChannel} with the static methods on
 * {@link AsyncChannels}.
 * <p>
 * This interface represents an <i> unbounded </i> queue, meaning there is no mechanism to notify
 * the sender that the queue is "full" (nor is there a notion of the queue being full to begin
 * with...). The channel will continue to accept values as fast as the senders can
 * {@link #send(Completed)} them, regardless of the rate at which the values are being consumed. If
 * senders produce a lot of values much faster than the consumption rate, it will lead to an OOM, so
 * users are responsible for enforcing that the channel does not grow too large. If you would like a
 * channel abstraction that provides backpressure, see {@link BoundedAsyncChannel}.
 * <p>
 * This channel can only be closed by someone calling {@link #close()}, it can be called by
 * consumers or senders. It is strongly recommended that all instances of this class eventually be
 * closed. Almost all terminal/consuming operations on {@link AsyncIterator} return futures that
 * require the iterator be closed before returning. An unclosed channel has a high probability of
 * introducing an uncompleted future on the reader's side. After the channel is closed, subsequent
 * {@link #send(Completed)}s are rejected, and consumption attempts return immediately with empty
 * futures.
 * <p>
 * Typically you'll want to use a channel when you have some "source" of items, and want to consume
 * them asynchronously as the become available. Some examples of sources could be a collection of
 * futures, bytes off of a socket, results produced by dedicated worker threads, etc. Suppose you
 * had scenario where you had many threads doing some CPU intensive computation, and you'd send
 * their answers off to some server somewhere one at a time.
 * 
 * <pre>
 * <code>
 * AsyncChannel<Integer, VoidException> channel = AsyncChannels.unbounded();
 * for (i = 0; i < numThreads; i++) {
 *   // spawn threads that send results to channel
 *   threadpool.submit(() -> {
 *      while (canStillCompute) {
 *        int num = computeReallyExpensiveThing();
 *        channel.send(Completed.success(Optional.of(num)));
 *      }
 *    }
 * }
 * 
 * //consumer of channel, sending numbers to a server one at a time
 * channel
 *   // lazily map numbers to send
 *   .map(number -> sendToServer(number))
 *   // consume all values
 *   .consume()
 *   // iteration stopped (meaning channel was closed)
 *   .onComplete(ig -> sendToServer("no more numbers!");
 * 
 * threadpool.awaitTermination();
 * // close the channel, done computing
 * channel.close();
 * 
 * </code>
 * </pre>
 * <p>
 * It is also convenient to use a channel to merge many {@link AsyncIterator}s together. Think if we
 * were the destination server in the previous example, and we had many compute servers sending us
 * numbers they were computing. If we used {@link AsyncIterators#concat(java.util.Collection)} in
 * the following example, we would wait until we got all the work from the first iterator to move
 * onto the next. With a channel we process each number as soon as it becomes available.
 * 
 * <pre>
 * <code>
 * AsyncIterator<Integer, VoidException> getNumbersFrom(ServerLocation ip);
 * AsyncChannel channel = AsyncChannels.unbounded();
 * futures = ips.stream()
 * 
 *   // get an AsyncIterator of numbers from each server
 *  .map(this::getNumbersFrom)
 *  
 *   // send each number on each iterator into the channel as they arrive
 *  .forEach(asyncIterator -> asyncIterator
 *      .forEach(t -> channel.send(Completed.success(Optional.of(t)))))
 *      
 *  // bundle futures into a list    
 *  .collect(Collectors.toList());
 *  
 *  // close the channel whenever we're done sending
 *  Futures.all(futures).onComplete(ig -> channel.close());
 *  
 *  // prints each number returned by servers as they arrive 
 *  channel
 *    .forEach(num -> System.out.println(num))
 *    .onComplete(ig -> System.out.println("finished getting all numbers")));
 * </code>
 * </pre>
 * <p>
 * A reminder, all topics addressed in the documentation of {@link AsyncIterator} apply to this
 * interface as well. Most importantly this means:
 * <li>The T values that are sent into this channel <b> cannot be null </b></li>
 * <li>Consumption of an AsyncIterator is <b> not </b> thread safe</li>
 * <li>Lazy methods on AsyncIterator like map/flatMap don't consume anything. Make sure you actually
 * use a consumption operation somewhere, otherwise no one will ever read what you send</li>
 * 
 * @param <T>
 * @param <E>
 * @see AsyncIterators
 * @see AsyncChannels
 * @see BoundedAsyncChannel
 */
public interface AsyncChannel<T> extends AsyncIterator<T> {
  /**
   * Send a value into this channel, that can be consumed via the {@link AsyncIterator} interface.
   * 
   * This method is thread safe - multiple threads can send values into this channel concurrently.
   * This channel is unbounded, so it will continue accept new items immediately and store them in
   * memory until they can be consumed. If you are sending working faster than you can consume it,
   * this can easily lead to an out of memory condition.
   * 
   * Note that calling {@link #close()} is the <b>only</b> way to close the channel. Specifically,
   * sending exceptions doesn't close the channel. See {@link #close()} for details.
   * 
   * @param item
   * @return true if the item was accepted, false if it was rejected because the channel has been
   *         closed
   */
  boolean send(T item);

  /**
   * Close the channel
   * 
   * After the channel is closed, all subsequent sends will be rejected, returning false. After the
   * consumer consumes whatever was sent before the close, the consumer will receive
   * Optional.empty().
   * <p>
   * Note that calling {@link #close()} is the <b>only</b> way to close the channel. Specifically,
   * sending exceptions doesn't close the channel. This is consistent with the interface on
   * {@link AsyncIterator}; While higher level methods generally stop iteration on exception or
   * empty, {@link AsyncIterator#nextFuture()} can still return exceptions and iteration may
   * continue. This allows users of AsyncChannel/AsyncIterator to continue iterating over possibly
   * exceptional values, at the cost of having to use nextFuture directly to do so.
   * 
   */
  void close();

  /**
   * Get a result from the channel if there is one ready right now.
   * 
   * This method consumes parts of the channel, so like the consumption methods on
   * {@link AsyncIterator}, this method should be used in a single threaded fashion. After
   * {@link #close()} is called and all outstanding results are consumed, poll will always return
   * empty. <br>
   * Notice that the channel being closed is indistinguishable from the channel being transiently
   * empty. To discover that no more results will ever be available, you must use the normal means
   * on {@link AsyncIterator}: Either calling {@link #nextFuture()} and seeing if the result is
   * empty when the future completes, or using one of the consumer methods that only complete once
   * the channel has been closed.
   * 
   * @return A value if there was one immediately available in the channel, empty if the channel is
   *         currently empty.
   */
  Optional<T> poll();
}


