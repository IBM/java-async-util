/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.examples.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.ibm.asyncutil.iteration.AsyncIterator;
import com.ibm.asyncutil.locks.AsyncLock;
import com.ibm.asyncutil.util.Combinators;
import com.ibm.asyncutil.util.StageSupport;

public class Locks {

  static class Requester {
    private final AsyncLock lock;
    private final AsynchronousSocketChannel connectedChannel;

    Requester(final AsynchronousSocketChannel channel) {
      this.lock = AsyncLock.create();
      this.connectedChannel = channel;
    }

    /**
     * Send a request to the server
     *
     * @param i the request to send
     * @return a stage that will complete with the server's response
     */
    CompletionStage<Integer> intRequest(final int i) {
      // acquire the lock, unconditionally closing the token after the stage we produced completes
      return StageSupport.tryComposeWith(this.lock.acquireLock(), token -> NioBridge

          // write the int into the channel
          .writeInt(this.connectedChannel, i)

          // read the int response from the server
          .thenCompose(ignore -> NioBridge.readInt(this.connectedChannel)));
    }
  }

  /**
   * Setup a server that will accept a connection from a single client, and then respond to every
   * request sent by the client by incrementing the request by one.
   * 
   * @return
   * @throws IOException
   */
  static SocketAddress setupServer() throws IOException {
    final AsynchronousServerSocketChannel server =
        AsynchronousServerSocketChannel.open().bind(null);

    final SocketAddress addr = server.getLocalAddress();

    NioBridge.accept(server).thenAccept(channel -> {
      AsyncIterator
          .generate(() -> NioBridge.readInt(channel))
          .thenCompose(clientIntRequest -> NioBridge.writeInt(channel, clientIntRequest + 1))
          .consume()
          .whenComplete((ignore, ex) -> {
            System.out.println("connection closed, " + ex.getMessage());
          });
    });
    return addr;
  }

  public static void main(final String[] args) throws IOException {
    final SocketAddress addr = setupServer();

    final Collection<Integer> responses = NioBridge.connect(addr)
        .thenCompose(connectedChannel -> {
          final Requester requester = new Requester(connectedChannel);

          // send 5 requests using the requester object. The requester object will internally make
          // sure that these are sent sequentially and request/responses are not mixed.
          final List<CompletionStage<Integer>> stages =
              IntStream
                  .range(0, 5)
                  .mapToObj(i -> requester.intRequest(i))
                  .collect(Collectors.toList());
          return Combinators.collect(stages);
        }).toCompletableFuture().join();

    System.out.println(responses);
  }

}
