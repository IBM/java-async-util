package com.ibm.async_util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestUtil {

  static <T> T join(final CompletionStage<T> future) {
    return future.toCompletableFuture().join();
  }

  static <T> T join(final CompletionStage<T> future, long time, TimeUnit timeUnit) {
    try {
      return future.toCompletableFuture().get(time, timeUnit);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new CompletionException(e);
    }
  }

}
