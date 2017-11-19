/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.flow;


import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;

import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowSubscriberBlackboxVerification;

public class AdaptedBlackBoxSubscriberVerificationTest
    extends FlowSubscriberBlackboxVerification<Integer> {

  public AdaptedBlackBoxSubscriberVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  public Subscriber<Integer> createFlowSubscriber() {
    return new FlowAdapter.SubscribingIterator<Integer>() {
      @Override
      public void onSubscribe(final Flow.Subscription subscription) {
        super.onSubscribe(subscription);
        consume();
      }
    };
  }

  @Override
  public Integer createElement(final int element) {
    return element;
  }
}
