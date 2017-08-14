/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestUtil {

  public static <T> T join(final CompletionStage<T> future) {
    return future.toCompletableFuture().join();
  }

  public static <T> T join(final CompletionStage<T> future, final long time,
      final TimeUnit timeUnit) throws TimeoutException {
    try {
      return future.toCompletableFuture().get(time, timeUnit);
    } catch (InterruptedException | ExecutionException e) {
      throw new CompletionException(e);
    }
  }
}
