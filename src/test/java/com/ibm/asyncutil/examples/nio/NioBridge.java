/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.examples.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.ibm.asyncutil.util.StageSupport;

public class NioBridge {

  static CompletionStage<AsynchronousSocketChannel> accept(
      final AsynchronousServerSocketChannel server) {
    final CompletableFuture<AsynchronousSocketChannel> channelFuture = new CompletableFuture<>();
    server.accept(channelFuture,
        new CompletionHandler<AsynchronousSocketChannel, CompletableFuture<AsynchronousSocketChannel>>() {
          @Override
          public void completed(final AsynchronousSocketChannel result,
              final CompletableFuture<AsynchronousSocketChannel> attachment) {
            attachment.complete(result);
          }

          @Override
          public void failed(final Throwable exc,
              final CompletableFuture<AsynchronousSocketChannel> attachment) {
            attachment.completeExceptionally(exc);
          }
        });
    return channelFuture;
  }

  static CompletionStage<AsynchronousSocketChannel> connect(final SocketAddress addr) {
    try {
      final AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
      return connect(channel, addr).thenApply(ig -> channel);
    } catch (final IOException e) {
      return StageSupport.exceptionalStage(e);
    }
  }

  static CompletionStage<Void> connect(final AsynchronousSocketChannel channel,
      final SocketAddress addr) {
    final CompletableFuture<Void> connectFuture = new CompletableFuture<>();
    channel.connect(addr, connectFuture, new CompletionHandler<Void, CompletableFuture<Void>>() {
      @Override
      public void completed(final Void result, final CompletableFuture<Void> attachment) {
        attachment.complete(null);
      }

      @Override
      public void failed(final Throwable exc, final CompletableFuture<Void> attachment) {
        attachment.completeExceptionally(exc);
      }
    });
    return connectFuture;
  }

  static CompletionStage<Void> writeInt(final AsynchronousSocketChannel channel,
      final int toWrite) {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(toWrite);
    buffer.flip();
    final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
    channel.write(buffer, writeFuture,
        new CompletionHandler<Integer, CompletableFuture<Void>>() {
          @Override
          public void completed(final Integer result, final CompletableFuture<Void> attachment) {
            if (result != 4)
              attachment.completeExceptionally(new IOException("write interrupted"));
            else
              attachment.complete(null);
          }

          @Override
          public void failed(final Throwable exc, final CompletableFuture<Void> attachment) {
            attachment.completeExceptionally(exc);
          }
        });
    return writeFuture;
  }

  static CompletionStage<Integer> readInt(final AsynchronousSocketChannel channel) {
    final ByteBuffer buf = ByteBuffer.allocate(4);
    final CompletableFuture<Integer> intFuture = new CompletableFuture<>();
    channel.read(buf, buf, new CompletionHandler<Integer, ByteBuffer>() {
      @Override
      public void completed(final Integer result, final ByteBuffer attachment) {
        attachment.flip();
        intFuture.complete(attachment.getInt());
      }

      @Override
      public void failed(final Throwable exc, final ByteBuffer attachment) {
        intFuture.completeExceptionally(exc);
      }
    });
    return intFuture;
  }

  public static void main(final String[] args) throws IOException {
    final AsynchronousServerSocketChannel server =
        AsynchronousServerSocketChannel.open().bind(null);

    final CompletionStage<AsynchronousSocketChannel> acceptStage = accept(server);
    final SocketAddress addr = server.getLocalAddress();
    final CompletionStage<AsynchronousSocketChannel> connectStage = connect(addr);

    // after connecting, write the integer 42 to the server
    final CompletionStage<Void> writeStage =
        connectStage.thenAccept(channel -> writeInt(channel, 42));

    final CompletionStage<Void> readStage = acceptStage
        // after accepting, read an int from the socket
        .thenCompose(NioBridge::readInt)
        // print the result
        .thenAccept(System.out::println);

    // wait for the write and the read to complete
    writeStage.toCompletableFuture().join();
    readStage.toCompletableFuture().join();
  }

}
