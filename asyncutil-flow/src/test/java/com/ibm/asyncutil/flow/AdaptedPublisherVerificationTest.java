/*
 * Copyright (c) IBM Corporation 2017. All Rights Reserved.
 * Project name: java-async-util
 * This project is licensed under the Apache License 2.0, see LICENSE.
 */

package com.ibm.asyncutil.flow;

import java.util.concurrent.Flow.Publisher;

import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;

import com.ibm.asyncutil.iteration.AsyncIterator;

public class AdaptedPublisherVerificationTest extends FlowPublisherVerification<Integer> {

  public AdaptedPublisherVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  public Publisher<Integer> createFlowPublisher(final long l) {
    AsyncIterator<Integer> it = AsyncIterator.repeat(1);
    // infinite on MAX_VALUE
    if (l != Long.MAX_VALUE) {
      it = it.take(l);
    }
    return FlowAdapter.toPublisher(it);
  }

  @Override
  public Publisher<Integer> createFailedFlowPublisher() {
    // return ReactiveStreamsConverter.toPublisher(AsyncIterator.error(new RuntimeException("test
    // error")));
    // null ignores these tests. An iterator's error is lazy (requires a request to get an error),
    // but there are two tests that test for an error on subscription
    return null;
  }
}
