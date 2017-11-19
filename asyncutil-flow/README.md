# asyncutil-flow

## Introduction

This optional module allows Java 9 users to translate the iteration primitive provided by asyncutil to and from [java.util.Flow constructs](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/Flow.html). An asyncutil [AsyncIterator](https://pages.github.ibm.com/cs-team-atg/async-util/apidocs/com/ibm/asyncutil/iteration/AsyncIterator.html) can be used as the source for a `Flow.Publisher`, and a `Flow.Publisher` can be consumed via an `AsyncIterator`.

## Downloading
TODO maven instructions

## Comparison to other Flow providers
There are now several options for implementations of Flow. Here are some things to consider when comparing `AsyncIterator` to other implementations, especially RxJava.

### CompletionStage oriented
asyncutil is strictly a Java 8+ library, and is designed with `CompletionStage` in mind. AsyncIterators are naturally constructed from stage producing APIs and always reduce back into stages when actually evaluated. Moreover, these terminal reduction steps take on the familiar form of `Stream` terminal methods. RxJava doesn't target CompletionStages, and is a better choice when supporting older Java releases.

### Iteration vs Reactivity
AsyncIterators excel at being local scope notions for structuring an iterative async computation and getting a `CompletionStage` of the result using a terminal method. It is well suited for concisely expressing an iterative asynchronous computation. Using it to model signals in a reactive style gets messy because you must carefully manage partially evaluating the iterator, which is a more natural fit for RxJava.

### Hot/Cold vs Lazy
In the Rx world there are two choices for an `Observable`.  Hot observables usually model sources that already "exist" and are always producing. Subscribers join into the existing stream. Cold observables constructa new stream of production when a subscriber is added.

Instead, an `AsyncIterator` is lazy, which is a constrained form of cold. It can only be consumed once, and production only starts upon consumption. This makes it very natural to model stateful streams, and you can build back to the cold observables model with `Supplier<AsyncIterator>`. Consumption directly includes a notion of completion (consumption is accomplished via terminal methods that return `CompletionStage`), compared to subscription in Rx. AsyncIterators are generally poorly suited for the Hot observable model.

### Constrained scope
AsyncIterators exclusively solve the problem of consumer driven sequential iteration, whereas RxJava provides a wealth of features like Schedulers, Plugins, flexible backpressure, etc. If `AsyncIterator` can sufficiently covers your needs, you may find it easier to work with.
