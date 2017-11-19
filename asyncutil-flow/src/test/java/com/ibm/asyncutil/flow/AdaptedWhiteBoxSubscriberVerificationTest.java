/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.flow;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowSubscriberWhiteboxVerification;

public class AdaptedWhiteBoxSubscriberVerificationTest
    extends FlowSubscriberWhiteboxVerification<Integer> {
  public AdaptedWhiteBoxSubscriberVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  protected Subscriber<Integer> createFlowSubscriber(
      final WhiteboxSubscriberProbe<Integer> probe) {
    final Subscriber<Integer> backing = new FlowAdapter.SubscribingIterator<>();
    return new Subscriber<Integer>() {
      @Override
      public void onSubscribe(final Subscription s) {
        backing.onSubscribe(s);

        probe.registerOnSubscribe(new SubscriberPuppet() {

          @Override
          public void triggerRequest(final long elements) {
            s.request(elements);
          }

          @Override
          public void signalCancel() {
            s.cancel();
          }
        });
      }

      @Override
      public void onNext(final Integer integer) {
        backing.onNext(integer);
        probe.registerOnNext(integer);
      }

      @Override
      public void onError(final Throwable throwable) {
        backing.onError(throwable);
        probe.registerOnError(throwable);
      }

      @Override
      public void onComplete() {
        backing.onComplete();
        probe.registerOnComplete();
      }
    };
  }

  @Override
  public Integer createElement(final int element) {
    return element;
  }
}
