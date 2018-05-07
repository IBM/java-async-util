# async-util

## Introduction
async-util is a library for working with Java 8 [CompletionStages](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html). Its primary goal is to provide tools for asynchronous coordination, including iterative production/consumption of CompletionStages and non-blocking asynchronous mutual exclusion support.

The library is broken up into three packages:
* [Locks](#locks)
* [Iteration](#iteration)
* [Util](#util)

To get started, you can browse the [javadocs](https://ibm.github.io/java-async-util/apidocs/overview-summary.html) or walk through some [example code](asyncutil/src/test/java/com/ibm/asyncutil/examples/nio/nio.md).

## Downloading

To add a dependency on asyncutil
```xml
<dependency>
    <groupId>com.ibm.async</groupId>
    <artifactId>asyncutil</artifactId>
    <version>0.1.0</version>
</dependency>
```

To get support for [Flow](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/Flow.html) (JDK9+ only)
```xml
<dependency>
    <groupId>com.ibm.async</groupId>
    <artifactId>asyncutil-flow</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Locks
The locks package provides asynchronous analogs of familiar synchronization primitives, all with efficient non-blocking implementations. Imagine we again have some source of asynchronity (say asynchronous network requests), and we'd like to implement an asynchronous method that makes a request and generates a result based on the request's response and some state that requires access under mutual exclusion.
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
If we wrap the `mutableState` operations in a `synchronized` block, we'll end up blocking the thread pool that runs our network operations. This is especially undesirable if this threadpool is possibly serving other interests in our application. We could solve that by creating our own thread pool just to do the locking + state manipulation using `thenApplyAsync` but that has a number of downsides 
* We've added more threads to our application for little benefit 
* If there's lock contention, we'll also be incurring a lot of additional context switching on these threads
* If many locations in our application solve similar problems, they'll also have to create their own thread pools which is not scalable. 

Instead we can use `AsyncLock` to provide exclusive access to the `MutableState`. We will try to acquire the lock on the thread that completes the network operation stage, and if it is not available we'll receive a CompletionStage that will notify us when it becomes available.

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
for cleanliness, we can use `StageSupport.tryWith` for try-with-resources emulation:
```java
CompletionStage<Result> makeRequest(Request request) {
  return asyncNetworkOperation(request)
    .thenCompose(response ->
      StageSupport.tryWith(lock.acquireLock(), ignored -> {
          mutableState.update(response);
          return mutableState.produceResult();
      })
    );
}
```
The package provides asynchronous versions of read/write locks, stamped locks, semaphores and named locks. The full [locks javadoc](https://ibm.github.io/java-async-util/apidocs/com/ibm/asyncutil/locks/package-summary.html) contains more information.

## Iteration
The classes in this package provide ways to generate and consume results asynchronously. The main mechanism is `AsyncIterator` interface, which can be considered an asynchronous analog of the Stream API. The full [iteration javadocs](https://ibm.github.io/java-async-util/apidocs/com/ibm/asyncutil/iteration/package-summary.html) contains more information on `AsyncIterator` as well as other asynchronous iteration constructs.

Consider the following example from the `Stream` documentation
```java
int sum = widgets.stream()
  .filter(w -> w.getColor() == RED)
  .mapToInt(w -> w.getWeight())
  .sum();
```
Say widgets was not a concrete collection, but instead generating a widget involved an asynchronous network request (or an expensive CPU computation, etc). If we instead make the source of widgets an `AsyncIterator` we can asynchronously apply the pipeline every time a widget becomes available, and return a CompletionStage which will be complete when the pipeline has finished. In this example, let's say we are only interested in the first 100 red widgets.
```java
// make an asynchronous network request that yields a widget
CompletionStage<Widget> getWidget();

CompletionStage<Integer> sum = AsyncIterator
  .generate(() -> getWidget())
  .filter(w -> w.getColor() == RED)
  .take(100)
  .thenApply(w -> w.getWeight())
  .collect(Collectors.summingInt(i -> i));
```
This will make one `getWidget` request at a time, running the rest of the pipeline operations each time a widget is generated on whatever thread processes the response required to generate the widget. When the widget stream is finished (in this case, after receiving 100 red widgets), the CompletionStage `sum` will complete with the result of the reduction operation. `AsyncIterators` have many other capabilities; if getting the weight required asynchronity we could use `thenCompose` instead of `thenApply`, if we needed to collect the weights into a collection we could use `collect(Collector)`, etc. 

It's often limiting to only be able to produce results for consumption iteratively. `AsyncQueue` provides ways to produce these values in parallel without any blocking synchronization:
```java
// implements AsyncIterator
AsyncQueue widgets = AsyncQueues.unbounded();

// dedicate NUM_THREADS threads to producing widgets
for (int i = 0; i < NUM_THREADS; i++) {
  executor.submit(() -> {
    // send returns whether the queue is still accepting values
    while (widgets.send(expensiveComputeWidget());
  });
}

// create a pipeline the same way as before
CompletionStage<Integer> sum = widgets.filter(...)...;

// once we get our sum, we can terminate the queue, stopping widget production
sum.thenRun(() -> widgets.terminate());
```

## Util
The util package contains interfaces and static functions for commonly reimplemented `CompletionStage` patterns. The best way to discover them all is to browse [the javadoc](https://ibm.github.io/java-async-util/apidocs/com/ibm/asyncutil/util/package-summary.html).

### `StageSupport`
StageSupport contains miscellaneous utility functions, including methods to create already completed exceptional stages, common transformations, and methods for working with resources that need to be asynchronously released.

### `AsyncCloseable` 
An interface analogous to [AutoCloseable](https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html) for objects that hold resources that must be relinquished asynchronously. By implementing `AsyncCloseable` with your own objects, you can take advantage of the `try*` methods on `StageSupport` to safely relinquish resources after performing asynchronous actions.

### `Combinators`
Static functions for combining collections of stages into a single result stage. The Java standard library provides two such methods with [CompletableFuture.allOf](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html#allOf-java.util.concurrent.CompletableFuture...-) / [CompletableFuture.anyOf](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html#anyOf-java.util.concurrent.CompletableFuture...-) . Unfortunately, these only work with arrays of `CompletableFuture`, not `CompletionStage`, so you must first convert using `toCompletableFuture` if you wish to use these methods. Collections must be converted to arrays well. The methods on `Combinators` work on `CompletionStage` and collections directly, furthermore several additional useful combinators are added. For example, to get the results of multiple stages with `CompletableFuture.allOf`:
```java
CompletableFuture<Integer>[] arr = ...
CompletionStage<List<Integer>> listFuture = CompletableFuture.allOf(arr).thenApply(ignore -> {
  final List<Integer> ints = new ArrayList<>();
  for (CompletableFuture<Integer> stage : arr) {
    return ints.add(stage.join());
  }
  return ints;
}
```
Instead, you can use `collect` on `Combinators`:
```java
Collection<CompletionStage<Integer>> stages = ...
CompletionStage<List<Integer>> listFuture = Combinators.collect(stages, Collectors.toList());
```

## Contributing
Contributions welcome! See [Contributing](CONTRIBUTING.md) for details.
