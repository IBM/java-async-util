package com.ibm.async_util.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestUtil {

  public static <T> T join(final CompletionStage<T> future) {
    return future.toCompletableFuture().join();
  }

  public static <T> T join(final CompletionStage<T> future, long time, TimeUnit timeUnit) throws TimeoutException{
    try {
      return future.toCompletableFuture().get(time, timeUnit);
    } catch (InterruptedException | ExecutionException e) {
      throw new CompletionException(e);
    }
  }
}
