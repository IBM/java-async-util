async-util with nio
============

[Java NIO](https://docs.oracle.com/javase/7/docs/api/java/nio/package-summary.html) provides abstractions for doing IO in java. Of particular interest are mechanims for [nonblocking file and socket operations](https://docs.oracle.com/javase/7/docs/api/java/nio/channels/package-summary.html). We'll use the [AsynchronousChannel API](https://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousChannel.html) with sockets to get some familiarity with the async-util toolbox.

# NIO Bridge

NIO landed in Java 7, so the `CompletionStage` interface wasn't available. Instead, asynchrony can be acheived on  `AsynchronousChannel` with a callback scheme. Methods take the form

```java
void operation(... A attachment, CompletionHandler<V,? super A> handler)
```

where the [CompletionHandler interface](https://docs.oracle.com/javase/7/docs/api/java/nio/channels/CompletionHandler.html) has some methods to implement that will be called when the operation completes. We can easily bridge this to an API that's easier to use by using `CompletionStage`. In our example, we'd like methods to perform 4 socket related operations.

1. `accept` The server should be asynchronously notified when a new connection from a client is established
2. `connect` The client should be able to asynchronously connect to a server 
3. `read` Read some bytes from a socket
4. `write` Write some bytes to a socket

Let's start with the `accept` method. We'll first need a `AsynchronousServerSocketChannel`, which can be constructed with
```java
final AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open().bind(null);
```
With the `bind` call, this server is already listening for connections. By passing `null` to `bind`, a local address is automatically assigned. We're now ready to write an `accept` method that asynchronously gets a channel.

```java
static CompletionStage<AsynchronousSocketChannel> accept(final AsynchronousServerSocketChannel server) {
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
```

Whenever the connection is accepted, we simply complete the returned stage with the `AsynchronousSocketChannel`. We can write similar bridges for our remaining three methods. For writing and reading, let's simplify our communication protocol and only read and write 4 byte integer values. So in the end we'd like 4 methods with the following signatures:
```java
static CompletionStage<AsynchronousSocketChannel> connect(final SocketAddress addr);
static CompletionStage<AsynchronousSocketChannel> accept(final AsynchronousServerSocketChannel server);
static CompletionStage<Void> writeInt( final AsynchronousSocketChannel channel, final int toWrite);
static CompletionStage<Integer> readInt(final AsynchronousSocketChannel channel);
```
You can see the implementations of these methods in the `NioBridge` class.

To make sure it works, let's spin up a server and a client and write one value to the server, then read and print it on the client.

```java
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
```

> 42

Great! now we can start using our tools.

# AsyncIterator

Let's start by sending more than just one message to the server. We'll use an `AsyncIterator` to represent a stream of messages being sent by the client. The trick here is that we'd like to avoid writing our next integer until the write of the previous has asynchronously completed. The channel documentation indicates writing while a write is already pending may throw an exception on some implementations. We would like the equivalent of this
```java
for (int i : intsToSend) {
    writeInt(channel, i).toCompletableFuture().join();
}
```
but without blocking! 

We can start with an `AsyncIterator` representing writes. For now, let's just randomly generate the ints we'd like to send. Since this is cheap, we can just do it with an immediately complete CompletionStage, but if we needed to generate the integer asynchronously we could do that as well.

```java
static CompletionStage<Void> write100Randoms(final AsynchronousSocketChannel channel) {
    AsyncIterator<Void> writeIterator = AsyncIterator
        .generate(() -> StageSupport.completedStage(ThreadLocalRandom.current().nextInt(0, 100)))
        .thenCompose(i -> writeInt(channel, i));
}
```
Note that this is _lazy_, this code doesn't actually do anything yet until we start evaluating elements using some terminal operation (just like with `java.util.Stream`). We don't want to be writing for ever, so let's put a limit on it:
```java
AsyncIterator<Void> writeIterator = AsyncIterator
    .generate(() -> StageSupport.completedStage(ThreadLocalRandom.current().nextInt(0, 100)))
    .thenCompose(i -> writeInt(channel, i))
    .take(100)
```
Finally, we want to actually do the work. Let's apply a terminal operation. Since all we want to do is consume all the elements of iteration, we can just use `AsyncIterator::consume`, which returns a stage that completes when the iterator has been exhausted (in this case, after 100 writes complete). Let's wrap it all up in a method while we're at it.

```java
CompletionStage<Void> allItemsWritten = AsyncIterator
    .generate(() -> StageSupport.completedStage(ThreadLocalRandom.current().nextInt(0, 100)))
    .thenCompose(i -> writeInt(channel, i))
    .take(100)
    .consume();
```
This code will generate a random number, then write it to the socket. Whenever that write completes, the next random number will be generated then written to the socket, and so on until 100 random numbers have been written.

Our old reader only read a single element. Let's convert it to read a stream of elements. This is pretty easy, we can just do 
```java
AsyncIterator<Integer> readIterator = AsyncIterators.generate(() -> readInt(channel));
```
Now we have an `AsyncIterator` which (on evaluation) will read a value from the socket, and only read the next one when the previous one has finished. This is especially important if the channel only allows one read to be outstanding at a time. We again have the problem that if we evaluate this iterator we'll be reading forever. We could just limit the amount of items we're willing to read, but instead lets let the client decide when enough has been sent. Since the client is producing non-negative integers, we can say the reader can stop reading from the socket when it recieves -1.

```java
AsyncIterator<Integer> readIterator = AsyncIterators.generate(() -> readInt(channel)).takeWhile(i -> i != -1);
```

Alright, let's evaluate this iterator. Since we probably care about what the client is sending us, let's collect all the values that the client sends us into a list. We can use the familiar [Collectors API](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collector.html).

```java
CompletionStage<List<Integer>> readResult = AsyncIterators
    .generate(() -> readInt(channel))
    .takeWhile(i -> i != -1)
    .collect(Collectors.toList());
```
Now we will read integer values from a client one at a time and collect them into a list, and we have a stage that will be completed with that list when we have recieved a -1 from the client.

Oops, we need to actually send -1 - let's just send it after `allItemsWritten` completes.
```java
CompletionStage<Void> negativeOneSent = allItemsWritten.thenCompose(ignore -> writeInt(channel, -1));
```

Our 100 int sender is complete! Code available in the `Iteration` class in the examples package.

## Multiple connections

Let's enhance our client server example by supporting multiple connections. Here, we'll have our server be able to accept integers from four connected clients, and we'll create four connections on the client side.

### Client
Changing the client is pretty easy. Instead of connecting to the server address once, we'll do it four times, and after each connection we will call our method to write 100 random ints.

```java
for (int i = 0; i < 4; i++) {
  connect(addr).thenComposeAsync(channel -> write100Randoms(channel));
}
```
Since the connect and write method calls will happen asynchronously, all four writers could be writing at once. For simplicity we won't inspect the results of these stages when they complete, but we could if we wanted to verify our integers were sent as planned.

### Server

The server is slightly more complex. First, we'll have to make repeated calls to accept on a single `AsynchronousServerSocketChannel`. The `accept` documentation indicates we can only have one outstanding accept request at a time:
> Throws: AcceptPendingException - If an accept operation is already in progress on this channel

We can again make use of `AsyncIterator` to sequence the calls to accept. Let's start with an `AsyncIterator` of our four `accept` calls.

```java
final AsyncIterator<AsynchronousSocketChannel> clientConnections = AsyncIterator

        // listen for next connection
        .generate(() -> accept(server))

        // only will take 4 connections
        .take(4);
```

When we apply a terminal operation to this iterator, we will call accept 4 times and yield 4 connected SocketChannels, only making the next call to accept when the previous call has finished.

Now that we have our `accept` calls sorted out, let's figure out how to process our messages. We have many options, the best way to deal with each stream of incoming messages is heavily use case dependant. Ideally, we would like to avoid the sever business logic from having to deal with the concurrent reception of messages. For example, if we were writing these messages to a file or to an in memory data structure we'd like to avoid having to use locks. We can instead use the `AsyncChannel` primitive to pipe these 4 sources of messages into a single `AsyncIterator` that we can then consume sequentially without having to explicitly deal with concurrency. Given an AsyncIterator of messages (which we've seen how to get from an AsynchronousSocketChannel in the previous section) and an AsyncChannel, we can pipe messages into the channel with `AsyncChannel.send` like so.
```java
AsyncChannel<Integer> channel = AsyncChannels.unbounded();
AsyncIterator<Integer> requestIterator = ...
requestIterator
    .forEach(i -> channel.send(i))
    .thenRun(() -> System.out.println("all messages from " + requestIterator + " have made it into channel"));
``` 
Because `send` is thread safe, we can repeat this procedure with as many iterators of requests as we'd like. So now we have a general strategy:
1. Use `accept` in a sequential fashion to get four connected clients
2. When each client connection is established start routing all of that client's messages into the shared `AsyncChannel`
3. When all four client connections have finished sending their ints (indicated with -1), we can terminate the `AsyncChannel`
3. Consume the `AsyncChannel` sequentially like a standard `AsyncIterator`

```java
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
```

Now we can do whatever we want with the results channel.

```java
// do something with the results! - print each result as it comes from each client connection
results.forEach(i -> System.out.println(i)).toCompletableFuture().join();
```

Working code can be found in the `MultiProducerIteration` class in the examples package.

# Async locks

Using `AsyncIterator` for async concurrency control is nice but does not fit all possible patterns. They are particularly well suited when there is a unidirectional data flow (sort of like unix pipes), but there are many other ways that an asynchronous application may be structured.

## AsyncLock

Let's imagine our goal was not to accomplish some finite task like "sending 100 integers", but instead to write a class that provides a sending abstraction for our application that might be used from many places.

### Requester

```java
/**
* Send messages and receive responses on an AsynchronousSocketChannel.
* 
* This class will ensure that there can be at most one unanswered request sent into the AsynchronousSocketChannel.
*/
class Requester {
    private final AsynchronousSocketChannel connectedChannel;

    Requester(final AsynchronousSocketChannel channel) {
        this.connectedChannel = channel;
    }
    
    /**
     * Send a request to the server
     *
     * @param i the request to send
     * @return a stage that will complete with the server's response
     */
    CompletionStage<Integer> intRequest(final int i) {
        // TODO
    }
}
``` 

Well, we know how to send and read from an `AsynchronousSocketChannel` - but we'd like to make sure that after we send a request we don't send another request until we recieved the response for the previous request. Before we were using `AsyncIterator` to coordinate our access to the Socketchannel, but that may be more difficult here. Instead we can just use an `AsyncLock` to enforce an asynchronous version of mutual exclusion while still avoiding any thread explicit blocking. We simply need to acquire the lock before sending and only release after we've gotten the response.

```java
class Requester {
    ...
    AsyncLock lock = AsyncLock.create();
    
    ...
    CompletionStage<Integer> intRequest(final int i) {
        // first acquire the lock
        this.lock.acquireLock().thenCompose(lockToken -> 
          NioBridge
              
              // write the int into the channel
              .writeInt(this.connectedChannel, i)
              
              // read the int response from the server
              .thenCompose(ignore -> NioBridge.readInt(this.connectedChannel))
              
              // unconditionally release the lock
              .whenComplete((j, ex) -> lockToken.releaseLock()));
    }
    
}
```

Special care has to be made to always release the `lockToken` in all paths (including exceptional ones). A try/finally like analog exists on the `StageSupport` class to make this more convienent:

```java
// first acquire the lock, unconditionally closing the lock after the stage we produced
// completes
return StageSupport.tryComposeWith(this.lock.acquireLock(), token -> NioBridge

    // write the int into the channel
    .writeInt(this.connectedChannel, i)

    // read the int response from the server
    .thenCompose(ignore -> NioBridge.readInt(this.connectedChannel)));
```

Now we can freely share our `Requester` object, knowing that our scheme for sequential sends/receipts of messages will be perserved. Users will be free to call send without any external concurrency control, and can do so without ever blocking their threads of execution.

### Server
Just so we have a fully functional example, let's write some server code that accepts a single connection and responds to any integer request by returning the increment of the request.

```java
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
```
Full code for this example can be found in the `Locks` class in the examples package.

Achieving this sequential pattern of concurrency is simply one style of asynchronous concurrency that can be achieved with locks package. With a similar approach, it is easy to achieve concurrency with a limit (`AsyncSemaphore`), read/write concurrency (`AsyncReadWriteLock`), name based concurrency (`AsyncNamedLock`), and more.

## Epochs

One especially useful primitive in the locks package is `ObservableEpoch`. In all of our examples, we have not done a good job managing the resources of the connections we introduced, usually just leaving them open after we were finished with them. Let's modify the `Requester` object in the previous example to support closing the backing connection. A problem arises when we think about what should happen if user tries to initiate a request after close has been called. Furthermore, we'd also like to finish any requests from users who have already made requests before shutting down our connection. We can use `ObservableEpoch` to achieve these semantics.

```java
static class CloseableRequester implements AsyncCloseable {
    final ObservableEpoch epoch;
    final Locks.Requester requester;
    private final AsynchronousSocketChannel channel;

    CloseableRequester(final AsynchronousSocketChannel channel) {
      this.channel = channel;
      this.requester = new Locks.Requester(channel);
      this.epoch = ObservableEpoch.newEpoch();
    }

    /**
     * Send a request to the server if {@code this} requester has not yet been closed
     *
     * @param i the request to send
     * @return a present stage that will complete with the server's response, or empty if the
     *         requester has already been closed
     */
    Optional<CompletionStage<Integer>> intRequest(final int i) {
        // TODO
    }

    /**
     * Forbid any new {@link #intRequest(int)} from starting
     *
     * @return A stage that will complete when all currently outstanding {@link #intRequest(int)
     *         intRequests} have completed.
     */
    @Override
    public CompletionStage<Void> close() {
        // TODO
    }
  }
```

Let's first implement the intRequest. Now we must try to enter the `epoch` before calling the `intRequest` method on our backing `Requester` object. We do this by calling `enter`, which will return us a token if we successfully entered the epoch, or empty if the epoch had already been terminated.

```java
return this.epoch
    .enter()
    .map(epochToken -> this.requester
        .intRequest(i)
        .whenComplete((r, ex) -> epochToken.close()));
```
when our entire asynchronous operation is complete, we make sure to exit the epoch which will allow close attempts to complete.

Now we can implement close

```java
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
```
We first terminate the epoch, which will stop any new entrants from coming into it. Once the returned stage completes, the epoch is closed. There are no entrants currently in the epoch and no more will be let in. If we weren't the first to terminate had already been called, we don't do anything. Otherwise, we can clean up the resources associated with the requester, namely we may close our socket.

Full code for this example can be found in the `Epochs` class in the examples package.
