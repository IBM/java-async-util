# async-util

## Introduction
async-util is a library for working with Java 8 [CompletionStages](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html). Its primary goal is to provide tools for asynchronous coordination, including iterative production/consumption of CompletionStages and lock-free asynchronous mutual exclusion support.

The library is broken up into three packages:
* [Locks](#locks)
* [Iteration](#iteration)
* [Util](#util)

Working with plain CompletableFutures becomes more difficult when much of the chained work happens far removed from the source of asynchronity. Because of the care that needs to be taken to avoid blocking on certain thread pools, either user code must lose abstraction and be cognizant of where a computation is running, or be forced to spin up more threads and incur additional thread pool submission costs. Likewise, chained computations need to carefully manage the stack when asynchronous calls happen to complete synchronously to avoid StackOverflows. These tools make it easy to write high level asynchronous code that avoids explicitly dealing with these problems.

## Downloading
TODO maven instructions

## Iteration
The classes in this package provide ways to generate and consume results asynchronously. The main mechanism the is `AsyncIterator` interface, which can be considered an asynchronous analog of the Stream API. The full [iteration javadocs](https://pages.github.ibm.com/cs-team-atg/async-util/apidocs/com/ibm/async_util/iteration/package-summary.html) contain more information on `AsyncIterator` as well as other asynchronous iteration constructs.

Consider the following example from the `Stream` documentation
```java
int sum = widgets.stream()
  .filter(w -> w.getColor() == RED)
  .mapToInt(w -> w.getWeight())
  .sum();
```
Say widgets was not a concrete collection, but instead generating a widget was involved asynchronous network request (or an expensive CPU computation, etc). If we instead make the source of widgets an `AsyncIterator` we can asynchronously apply the pipeline every time a widget becomes available, and return a CompletionStage which will be complete when the pipeline has finished. In this example, let's say we are only interested in the first 100 red widgets.
```java
// make an asynchronous network request that yields a widget
CompletionStage<Widget> getWidget();

CompletionStage<Integer> sum = AsyncIterator
  .generate(() -> getWidget())
  .filter(w -> w.getColor() == RED)
  .take(100)
  .thenApply(w -> w.getWeight())
  .fold((sum, next) -> sum + next)
```
This will make one `getWidget` request at a time, running the rest of the pipeline operations each time a widget is generated on whatever thread processes the response required to generate the widget. When the widget stream is finished (in this case, after recieving 100 red widgets), the CompletionStage `sum` will complete with the result of the reduction operation. `AsyncIterators` have many other capabilities; if getting the weight required asynchronity we could use `thenCompose` instead of `thenApply`, if we needed to collect the weights into a collection we could use `collect(Collector)`, etc. 

It's often limiting to only be able to produce results for consumption iteratively. `AsyncChannel` provides ways to produce these values in parallel without any blocking synchronization:
```java
// implements AsyncIterator
AsyncChannel widgets = AsyncChannels.unbounded();

// dedicate NUM_THREADS threads to producing widgets
for (int i = 0; i < NUM_THREADS; i++) {
  executor.submit(() -> {
    // send returns whether the channel is still accepting values
    while (widgets.send(expensiveComputeWidget());
  });
}

// create a pipeline the same way as before
CompletionStage<Integer> sum = widgets.filter(...)...;

// once we get our sum, we can terminate the channel, stopping widget production
sum.thenRun(() -> widgets.terminate());
```

## Locks
The locks package provides asynchronous analogs of familiar synchronization primitives, all with efficient lock-free implementations. Imagine we again have some source of asynchronity (say asynchronous network requests), and we'd like to implement an asynchronous method that makes a request and generates a result based on the request's response and some state that requires access under mutual exclusion.
```java
class MyAsyncClass {
  // not thread safe
  private MutableState mutableState;
  
  private CompletionStage<Response> asyncNetworkOperation(Request request) {...}


  CompletionStage<Result> makeRequest(Request request) {
    return asyncNetworkOperation(request)
      .thenApply(response -> {
        // unsafe!
        mutableState.update(response);
        return mutableState.produceResult();
      });
  }
}
```
If we wrap the `mutableState` operations in a `synchronized` block, we'll end up blocking the thread pool that runs our network operations. This is especially undesirable if this threadpool is possibly serving other interests in our application. We could solve that by creating our own thread pool just to do the locking + state manipulation using `thenApplyAsync`, but now we've added more threads for little benefit. If there's actual contention, we'll also be incurring a lot of additional context switching. If many locations in our application solve similar problems, they'll also have to create their own thread pools which is not scalable. 
Instead we can use `AsyncLock` to provide exclusive access to the `MutableState`. We will try to acquire the lock on the thread that completes the network operation stage, and if it is not available we'll recieve a CompletionStage that will notify us when it becomes available.

```java
...
private AsyncLock lock = AsyncLock.create();

CompletionStage<Result> makeRequest(Request request) {
  return asyncNetworkOperation(request)
    .thenCompose(response ->
      lock.acquireLock().thenApply(token -> {
        try {
          mutableState.update(response);
          return mutableState.produceResult();
        } finally {
          token.release();
        }
      })
    });
}
```
for cleanliness, we can use the `FutureSupport.tryWith` try-with-resources emulation:
```java
CompletionStage<Result> makeRequest(Request request) {
  return asyncNetworkOperation(request)
    .thenCompose(response ->
      FutureSupport.tryWith(lock.acquireLock(), ignored -> {
          mutableState.update(response);
          return mutableState.produceResult();
      })
    );
}
```
The package provides lock-free asynchronous versions of read/write locks, stamped locks, semaphores and named locks. The full [locks javadoc](file:///Users/rkhadiwala/workspace/gh-fork/casn/async-util/target/site/apidocs/com/ibm/async_util/locks/package-frame.html) contains more information.
## Util
TODO
