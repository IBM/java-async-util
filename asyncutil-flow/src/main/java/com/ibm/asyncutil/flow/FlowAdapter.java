/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.flow;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.ibm.asyncutil.iteration.AsyncIterator;
import com.ibm.asyncutil.iteration.AsyncTrampoline;
import com.ibm.asyncutil.locks.AsyncLock;
import com.ibm.asyncutil.util.Either;
import com.ibm.asyncutil.util.StageSupport;

/**
 * Adapters to convert between {@link Flow.Publisher} and {@link AsyncIterator}.
 * 
 * @author Ravi Khadiwala
 */
public class FlowAdapter {
  private FlowAdapter() {}

  /**
   * Convert a {@link Flow.Publisher} into an {@link AsyncIterator}. Calling a terminal method on a
   * pipeline including the returned iterator will start a subscription on {@code publisher}.
   * Elements will be requested as the returned iterator is consumed, and the returned iterator will
   * stop iteration when the {@code publisher} {@link Subscriber#onComplete() finishes} or produces
   * an {@link Subscriber#onError(Throwable) error}.
   * <p>
   * Users of the returned {@link AsyncIterator} must call {@link AsyncIterator#close()} when they
   * have finished using the iterator so that {@code publisher} may clean up any associated
   * resources.
   * 
   * @param publisher that will be subscribed to in order to yield elements from the returned
   *        iterator
   * @return An {@link AsyncIterator} that will iterate over elements produced via a
   *         {@link Flow.Subscription} from the given {@code publisher}
   */
  public static <T> AsyncIterator<T> toAsyncIterator(final Flow.Publisher<? extends T> publisher) {
    return new SubscribingIterator<>(publisher);
  }

  /**
   * Convert an {@link AsyncIterator} into a {@link Flow.Publisher}. Because AsyncIterators are
   * single consumer, the returned publisher should only be subscribed to once. When
   * {@code asyncIterator} is exhausted or returns an exception the iterator will be
   * {@link AsyncIterator#close() closed} and {@link Subscriber} will be notified accordingly. If
   * the {@link Subscription} is cancelled before iterator is complete, the iterator be closed as
   * well.
   * <p>
   * Exceptions produced by either iteration or by close will be delivered via
   * {@link Subscriber#onError(Throwable)}. If both iteration and close produce exceptions, the
   * exception produced by close will be added as a suppressed exception to the iteration exception.
   * 
   * 
   * @param asyncIterator used to produce elements published by the returned {@link Publisher}
   * @return a {@link Publisher} that supports a single subscription that will yield elements from
   *         {@code asyncIterator}
   */
  public static <T> Flow.Publisher<T> toPublisher(final AsyncIterator<? extends T> asyncIterator) {
    return new IteratorBackedPublisher<>(asyncIterator);
  }

  /**
   * Convert a {@link Supplier} of {@link AsyncIterator AsyncIterators} into a
   * {@link Flow.Publisher}. Because AsyncIterators are single consumer, each subscription of the
   * returned publisher will generate a new AsyncIterator from {@code asyncIteratorSupplier}. When a
   * generated AsyncIterator is exhausted or returns an exception the iterator will be
   * {@link AsyncIterator#close() closed} and {@link Subscriber} will be notified accordingly. If
   * the {@link Subscription} is cancelled before iterator is complete, the iterator be closed as
   * well.
   * <p>
   * Exceptions produced by either iteration or by close will be delivered via
   * {@link Subscriber#onError(Throwable)}. If both iteration and close produce exceptions, the
   * exception produced by close will be added as a suppressed exception to the iteration exception.
   * 
   * @param asyncIteratorSupplier used to produce AsyncIterators of elements published by the
   *        returned {@link Publisher}
   * @return a {@link Publisher} that supports multiple subscriptions that will yield elements from
   *         AsyncIterators generated from {@code asyncIteratorSupplier}
   */
  public static <T> Flow.Publisher<T> toPublisher(
      final Supplier<AsyncIterator<? extends T>> asyncIteratorSupplier) {
    return new SuppliedIteratorBackedPublisher<>(asyncIteratorSupplier);
  }

  private static class SuppliedIteratorBackedPublisher<T> implements Flow.Publisher<T> {

    private final Supplier<AsyncIterator<? extends T>> asyncIteratorSupplier;

    private SuppliedIteratorBackedPublisher(
        final Supplier<AsyncIterator<? extends T>> asyncIteratorSupplier) {
      this.asyncIteratorSupplier = asyncIteratorSupplier;
    }

    @Override
    public void subscribe(final Flow.Subscriber<? super T> subscriber) {
      subscriber
          .onSubscribe(
              new IteratorBackedSubscription<>(this.asyncIteratorSupplier.get(), subscriber));
    }

    @Override
    public String toString() {
      return super.hashCode() + "[backed by " + this.asyncIteratorSupplier.toString() + "]";
    }
  }

  private static class IteratorBackedPublisher<T> implements Flow.Publisher<T> {
    private static final VarHandle SUBSCRIBED_HANDLE;
    static {
      try {
        final MethodHandles.Lookup l = MethodHandles.lookup();
        SUBSCRIBED_HANDLE =
            l.findVarHandle(IteratorBackedPublisher.class, "subscribed", boolean.class);
      } catch (final ReflectiveOperationException e) {
        throw new Error(e);
      }
    }

    private final AsyncIterator<? extends T> asyncIterator;
    @SuppressWarnings("unused") // accessed via varhandle
    private volatile boolean subscribed;

    private IteratorBackedPublisher(final AsyncIterator<? extends T> asyncIterator) {
      this.asyncIterator = asyncIterator;
    }

    @Override
    public void subscribe(final Flow.Subscriber<? super T> subscriber) {
      if ((boolean) SUBSCRIBED_HANDLE.getAndSet(this, true)) {
        subscriber.onError(new IllegalStateException(
            "Publisher " + this + " does not support multiple subscribers"));
        return;
      }
      subscriber.onSubscribe(new IteratorBackedSubscription<>(this.asyncIterator, subscriber));
    }

    @Override
    public String toString() {
      return super.toString() + "[backed by " + this.asyncIterator.toString() + "]";
    }
  }


  /*
   * package scope for tests
   */
  static class SubscribingIterator<T> implements Flow.Subscriber<T>, AsyncIterator<T> {
    final Flow.Publisher<? extends T> publisher;
    Flow.Subscription subscription;

    // calls to nextStage happen-before calls to onNext
    CompletableFuture<Either<End, T>> next = new CompletableFuture<>();

    /*
     * Users of this constructor must manually subscribe this Subscriber to a Publisher before
     * calling nextStage
     */
    SubscribingIterator() {
      this.publisher = null;
    }

    SubscribingIterator(final Flow.Publisher<? extends T> publisher) {
      this.publisher = Objects.requireNonNull(publisher);
    }

    @Override
    public CompletionStage<Either<End, T>> nextStage() {
      if (this.subscription == null) {
        // first call to nextStage, set subscription
        Objects.requireNonNull(this.publisher,
            "subscriber with no publisher must manually be subscribed before consumption")
            .subscribe(this);
      }
      final CompletableFuture<Either<End, T>> curr = this.next;
      this.subscription.request(1);
      return curr;
    }

    @Override
    public CompletionStage<Void> close() {
      if (this.subscription != null) {
        this.subscription.cancel();
      }
      return StageSupport.voidStage();
    }

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
      Objects.requireNonNull(subscription);
      if (this.subscription != null) {
        subscription.cancel();
        return;
      }
      this.subscription = subscription;
    }

    @Override
    public void onNext(final T item) {
      Objects.requireNonNull(item);
      final CompletableFuture<Either<End, T>> curr = this.next;
      this.next = new CompletableFuture<>();
      curr.complete(Either.right(item));
    }

    @Override
    public void onError(final Throwable throwable) {
      this.next.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      this.next.complete(End.end());
    }

    @Override
    public String toString() {
      return super.toString() + (this.subscription == null ? "[not-subscribed]"
          : "[subscription: " + this.subscription.toString() + "]");
    }
  }

  private static class IteratorBackedSubscription<T> implements Flow.Subscription {
    final AsyncIterator<T> iterator;
    final Flow.Subscriber<? super T> subscriber;
    final AsyncLock lock = AsyncLock.create();
    // whoever sets this field is responsible for calling close() (which requires the lock) and
    // notifying the subscriber of completion
    final AtomicBoolean finished = new AtomicBoolean();


    IteratorBackedSubscription(final AsyncIterator<T> iterator,
        final Flow.Subscriber<? super T> subscriber) {
      this.iterator = Objects.requireNonNull(iterator);
      this.subscriber = Objects.requireNonNull(subscriber);
    }

    @Override
    public void request(final long n) {
      StageSupport.tryComposeWith(this.lock.acquireLock(), token -> getn(n));
    }

    /**
     * Supply the subscriber with n elements, closing the iterator if iteration ends or throws.
     * Should be called under {@code lock}.
     * 
     * @param n number of elements
     * @return when we're finished fetching elements
     */
    private CompletionStage<Void> getn(final long n) {
      CompletionStage<Void> fillStage;
      if (n <= 0) {
        fillStage = StageSupport.exceptionalStage(
            new IllegalArgumentException("subscription requests must be positive"));
      } else {
        fillStage = AsyncTrampoline.asyncWhile(new Supplier<>() {
          long demand = n;

          @Override
          public CompletionStage<Boolean> get() {
            if (this.demand-- == 0 || IteratorBackedSubscription.this.finished.get()) {
              return StageSupport.completedStage(false);
            }
            return IteratorBackedSubscription.this.iterator
                .nextStage()
                .thenCompose(e -> e.fold(this::onEnd, this::onNext));
          }

          private CompletionStage<Boolean> onEnd(final AsyncIterator.End end) {
            if (finish()) {
              return IteratorBackedSubscription.this.iterator.close()
                  .handle((ig, closeEx) -> {
                    notifySubscriber(false, null, closeEx);
                    return false;
                  });
            }
            return StageSupport.completedStage(false);
          }

          private CompletionStage<Boolean> onNext(final T next) {
            IteratorBackedSubscription.this.subscriber.onNext(next);
            return StageSupport.completedStage(true);
          }
        });
      }
      return fillStage.exceptionally(e -> {
        if (finish()) {
          this.iterator.close().whenComplete((ig, closeEx) -> notifySubscriber(false, e, closeEx));
        }
        return null;
      });

    }

    private boolean finish() {
      return !this.finished.getAndSet(true);
    }

    private void notifySubscriber(
        final boolean cancelled,
        Throwable exception,
        Throwable closeException) {
      exception = unwrapCompletionException(exception);
      closeException = unwrapCompletionException(closeException);
      if (exception == null && closeException == null) {
        if (!cancelled) {
          this.subscriber.onComplete();
        }
      } else if (exception != null && closeException != null) {
        exception.addSuppressed(closeException);
        this.subscriber.onError(exception);
      } else if (exception != null) {
        this.subscriber.onError(exception);
      } else {
        this.subscriber.onError(closeException);
      }
    }

    private Throwable unwrapCompletionException(final Throwable throwable) {
      if (throwable instanceof CompletionException) {
        return throwable.getCause();
      }
      return throwable;
    }

    @Override
    public void cancel() {
      if (finish()) {
        StageSupport.tryComposeWith(this.lock.acquireLock(), token -> this.iterator
            .close()
            .whenComplete((ig, closeEx) -> notifySubscriber(true, null, closeEx)));
      }
    }
  }
}
