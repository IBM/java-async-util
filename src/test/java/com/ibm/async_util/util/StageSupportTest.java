/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.junit.Assert;
import org.junit.Test;

public class StageSupportTest {
  private static class TestException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  @Test
  public void testThenComposeOrRecover() {
    final CompletionStage<Integer> error = StageSupport.exceptionalStage(new TestException());
    final CompletionStage<Integer> success = StageSupport.completedStage(1);
    final CompletionStage<Integer> success2 = StageSupport.completedStage(2);

    // input stage status should only effect function arguments
    for (final boolean inputFailed : new boolean[] {false, true}) {
      final CompletionStage<Integer> inputStage = inputFailed ? error : success;
      final Integer expectedResult = inputFailed ? null : 1;

      {
        // successful compose
        final int x =
            StageSupport.thenComposeOrRecover(
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
            StageSupport.<Integer, Integer>thenComposeOrRecover(
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
            StageSupport.thenComposeOrRecover(
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
    } catch (final CompletionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException);
    }
  }
}
