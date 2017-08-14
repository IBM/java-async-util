/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

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

  /**
   * A {@link CompletionStage} wrapping interface which allows completing, and querying the result
   * of, a completion stage.
   * <p>
   * This interface is useful in unit tests to generalize over both {@link CompletableFuture} and
   * the validation-oriented {@link SimpleCompletionStage}
   * 
   * @param <T>
   */
  public interface CompletableStage<T> extends CompletionStage<T> {
    boolean complete(T t);

    boolean completeExceptionally(Throwable t);

    boolean isDone();

    default T join() {
      return toCompletableFuture().join();
    }

    default T join(final long time, final TimeUnit timeUnit) throws TimeoutException {
      try {
        return toCompletableFuture().get(time, timeUnit);
      } catch (InterruptedException | ExecutionException e) {
        throw new CompletionException(e);
      }
    }
  }

  private static class CompletableFutureAsCompletableStage<T> extends CompletableFuture<T>
      implements CompletableStage<T> {
  }

  /**
   * @return suppliers of the possible implementations of {@link CompletableStage}
   */
  public static <T> Stream<Supplier<CompletableStage<T>>> stageFactories() {
    return Stream.of(
        new Supplier<CompletableStage<T>>() {
          @Override
          public CompletableStage<T> get() {
            return new CompletableFutureAsCompletableStage<>();
          }

          @Override
          public String toString() {
            return "CompletableFuture factory";
          }
        },
        new Supplier<CompletableStage<T>>() {

          @Override
          public CompletableStage<T> get() {
            return new SimpleCompletionStage<>();
          }

          @Override
          public String toString() {
            return "SimpleCompletionStage factory";
          }
        });
  }

  /**
   * @return functions of the possible implementations of already-completed {@link CompletionStage}
   */
  public static <T> Stream<Function<Either<Throwable, T>, CompletionStage<T>>> doneStageFactories() {
    return Stream.<Function<Either<Throwable, T>, CompletionStage<T>>>concat(
        Stream.of(
            new Function<Either<Throwable, T>, CompletionStage<T>>() {

              @Override
              public CompletionStage<T> apply(final Either<Throwable, T> et) {
                return et.fold(StageSupport::exceptionalStage, StageSupport::completedStage);
              }

              @Override
              public String toString() {
                return "StageSupport factory";
              }
            }),
        TestUtil.<T>stageFactories()
            .<Function<Either<Throwable, T>, CompletionStage<T>>>map(
                factory -> new Function<Either<Throwable, T>, CompletionStage<T>>() {
                  @Override
                  public CompletionStage<T> apply(final Either<Throwable, T> et) {
                    final CompletableStage<T> c = factory.get();
                    et.forEach(c::completeExceptionally, c::complete);
                    return c;
                  }

                  @Override
                  public String toString() {
                    return "completed " + factory.toString();
                  }
                }));
  }
}
