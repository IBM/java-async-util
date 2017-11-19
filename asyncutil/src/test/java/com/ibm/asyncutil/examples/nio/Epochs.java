/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.examples.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import com.ibm.asyncutil.locks.AsyncEpoch;
import com.ibm.asyncutil.util.AsyncCloseable;

/**
 * Example using {@link AsyncEpoch} to wait for ongoing tasks to finish before cleaning up
 * resources
 */
public class Epochs {

  /**
   * Wrapper over a {@link com.ibm.asyncutil.examples.nio.Locks.Requester} that allows clients to
   * make {@link #intRequest(int) intRequests} until {@link #close()} is called
   */
  static class CloseableRequester implements AsyncCloseable {
    private final AsyncEpoch epoch;
    private final Locks.Requester requester;
    private final AsynchronousSocketChannel channel;

    CloseableRequester(final AsynchronousSocketChannel channel) {
      this.channel = channel;
      this.requester = new Locks.Requester(channel);
      this.epoch = AsyncEpoch.newEpoch();
    }

    /**
     * Send a request to the server if {@code this} requester has not yet been closed
     *
     * @param i the request to send
     * @return a present stage that will complete with the server's response, or empty if the
     *         requester has already been closed
     */
    Optional<CompletionStage<Integer>> intRequest(final int i) {
      return this.epoch
          .enter()
          .map(epochToken -> this.requester
              .intRequest(i)
              .whenComplete((r, ex) -> epochToken.close()));
    }

    /**
     * Forbid any new {@link #intRequest(int)} from starting
     *
     * @return A stage that will complete when all currently outstanding {@link #intRequest(int)
     *         intRequests} have completed.
     */
    @Override
    public CompletionStage<Void> close() {
      return this.epoch.terminate().thenAccept(closeSuccess -> {
        if (!closeSuccess) {
          System.out.println("close called multiple times");
          return;
        }
        try {
          this.channel.close();
        } catch (final IOException e) {
          throw new CompletionException(e);
        }
      });
    }
  }

  public static void main(final String[] args) throws IOException {
    final SocketAddress addr = Locks.setupServer();

    NioBridge
        .connect(addr)
        .thenCompose(connectedChannel -> {
          CloseableRequester requester = new CloseableRequester(connectedChannel);

          // send a request while the requester is open
          requester.intRequest(1).get().thenAccept(response -> {
            System.out.println(response);
          });

          // close the requester
          CompletionStage<Void> closeStage = requester.close();

          // won't be able to send any more requests
          if (requester.intRequest(2).isPresent()) {
            throw new IllegalStateException("impossible!");
          }

          return closeStage;
        })

        // wait for close to finish
        .toCompletableFuture().join();
  }

}
