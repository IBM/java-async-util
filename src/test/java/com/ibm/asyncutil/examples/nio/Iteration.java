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
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.ibm.asyncutil.iteration.AsyncIterator;
import com.ibm.asyncutil.util.StageSupport;

public class Iteration {

  /**
   * Write 100 random integers into {@code channel}, then write -1
   *
   * @param channel
   * @return A stage that completes when we've finished writing into the channel
   */
  static CompletionStage<Void> write100Randoms(final AsynchronousSocketChannel channel) {
    final CompletionStage<Void> allItemsWritten = AsyncIterator
        .generate(() -> StageSupport.completedStage(ThreadLocalRandom.current().nextInt(0, 100)))
        .thenCompose(i -> writeInt(channel, i))
        .take(100)
        .consume();
    return allItemsWritten.thenCompose(ignore -> writeInt(channel, -1));
  }

  /**
   * Read one int from the channel at a time.
   *
   * @param channel
   * @return A stage that completes when we read a -1 off of the channel
   */
  static CompletionStage<List<Integer>> readUntilStopped(
      final AsynchronousSocketChannel channel) {
    return AsyncIterator
        .generate(() -> readInt(channel))
        .takeWhile(i -> i != -1)
        .collect(Collectors.toList());
  }

  public static void main(final String[] args) throws IOException {
    final AsynchronousServerSocketChannel server =
        AsynchronousServerSocketChannel.open().bind(null);

    final CompletionStage<AsynchronousSocketChannel> acceptStage = accept(server);

    final SocketAddress addr = server.getLocalAddress();
    final CompletionStage<AsynchronousSocketChannel> connectStage = connect(addr);

    // after connecting, write 100 random integers, then write -1
    final CompletionStage<Void> writeStage =
        connectStage.thenCompose(channel -> write100Randoms(channel));

    final CompletionStage<List<Integer>> readStage =
        acceptStage.thenCompose(Iteration::readUntilStopped);

    // wait for the write and the read to complete, print read results
    writeStage.toCompletableFuture().join();
    System.out.println(readStage.toCompletableFuture().join());
  }
}
