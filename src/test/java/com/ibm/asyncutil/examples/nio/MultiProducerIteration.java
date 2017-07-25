/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.examples.nio;

import static com.ibm.asyncutil.examples.nio.NioBridge.accept;
import static com.ibm.asyncutil.examples.nio.NioBridge.connect;
import static com.ibm.asyncutil.examples.nio.NioBridge.readInt;
import static com.ibm.asyncutil.examples.nio.NioBridge.writeInt;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.ibm.asyncutil.iteration.AsyncChannel;
import com.ibm.asyncutil.iteration.AsyncChannels;
import com.ibm.asyncutil.iteration.AsyncIterator;
import com.ibm.asyncutil.util.Combinators;
import com.ibm.asyncutil.util.StageSupport;

public class MultiProducerIteration {

  static CompletionStage<Void> write100Randoms(final AsynchronousSocketChannel channel) {
    final CompletionStage<Void> allItemsWritten = AsyncIterator
        .generate(() -> StageSupport.completedStage(ThreadLocalRandom.current().nextInt(0, 100)))
        .thenCompose(i -> writeInt(channel, i))
        .take(100)
        .consume();
    return allItemsWritten.thenCompose(ignore -> writeInt(channel, -1));
  }

  /**
   * Given an AsyncIterator of {@link AsynchronousSocketChannel} representing connected clients,
   * return an AsyncIterator of messages from those clients, in whatever order they happen to arrive
   *
   * @param clientConnections An {@link com.ibm.asyncutil.iteration.AsyncIterator} of connected
   *        {@link AsynchronousSocketChannel sockets}
   * @return An {@link AsyncIterator} of messages from all clients in {@code clientConnections}
   */
  static AsyncIterator<Integer> routeClientMessages(
      final AsyncIterator<AsynchronousSocketChannel> clientConnections) {

    // we'll collect the results of all connections into this channel
    final AsyncChannel<Integer> results = AsyncChannels.unbounded();

    clientConnections
        .thenApply(socketChannel -> AsyncIterator

            // read ints from client one at a time
            .generate(() -> readInt(socketChannel))

            // stop when the client sends -1
            .takeWhile(i -> i != -1)

            // put each result into results as they arrive
            .forEach(results::send))

        // get a stage that completes with stages for each connection's routing task
        .collect(Collectors.toList())

        // returns a stage that completes when -1 has been returned on all connections
        .thenCompose(fillingCompleteStages -> Combinators.allOf(fillingCompleteStages))

        // when we've connected to 4 clients and read to -1 on all 4 of them, terminate our results
        // channel
        .thenRun(results::terminate);

    return results;

  }

  public static void main(final String[] args) throws IOException {
    final AsynchronousServerSocketChannel server =
        AsynchronousServerSocketChannel.open().bind(null);

    final SocketAddress addr = server.getLocalAddress();

    // on the client side, concurrently connect to addr 4 times, and write 100 random integers on
    // each connection
    for (int i = 0; i < 4; i++) {
      connect(addr).thenComposeAsync(channel -> write100Randoms(channel));
    }


    // on the server side, we'd like to accept 4 connections and route their messages into a single
    // place we can consume
    final AsyncIterator<AsynchronousSocketChannel> clientConnections = AsyncIterator

        // listen for next connection
        .generate(() -> accept(server))

        // only will take 4 connections
        .take(4);
    final AsyncIterator<Integer> results = routeClientMessages(clientConnections);


    // do something with the results! - print each result as it comes from each client
    results.forEach(i -> System.out.println(i)).toCompletableFuture().join();

  }
}
