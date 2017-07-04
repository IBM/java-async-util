package com.ibm.async_util.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

public class FutureSupportTest {
  private static class TestException extends RuntimeException {}

  @Test
  public void testThenComposeOrRecover() {
    final CompletionStage<Integer> error = FutureSupport.errorStage(new TestException());
    final CompletionStage<Integer> success = CompletableFuture.completedFuture(1);
    final CompletionStage<Integer> success2 = CompletableFuture.completedFuture(2);

    // input stage status should only effect function arguments
    for (boolean inputFailed : new boolean[] {false, true}) {
      CompletionStage<Integer> inputStage = inputFailed ? error : success;
      Integer expectedResult = inputFailed ? null : 1;

      {
        // successful compose
        int x =
            FutureSupport.thenComposeOrRecover(
                    inputStage,
                    (result, throwable) -> {
                      Assert.assertEquals(expectedResult, result);
                      Assert.assertEquals(inputFailed, throwable != null);
                      return success2;
                    })
                .toCompletableFuture()
                .join();
        Assert.assertEquals(2, x);
      }

      {
        // error compose with a thrown exception
        assertError(
            FutureSupport.<Integer, Integer>thenComposeOrRecover(
                inputStage,
                (result, throwable) -> {
                  Assert.assertEquals(expectedResult, result);
                  Assert.assertEquals(inputFailed, throwable != null);
                  throw new TestException();
                }));
      }

      {
        // error compose with a stage that completes with error
        assertError(
            FutureSupport.thenComposeOrRecover(
                inputStage,
                (result, throwable) -> {
                  Assert.assertEquals(expectedResult, result);
                  Assert.assertEquals(inputFailed, throwable != null);
                  return error;
                }));
      }
    }
  }

  private <T> void assertError(final CompletionStage<T> stage) {
    try {
      stage.toCompletableFuture().join();
      Assert.fail("expected exception");
    } catch (CompletionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException);
    }
  }
}
