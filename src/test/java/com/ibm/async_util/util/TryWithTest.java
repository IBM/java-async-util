package com.ibm.async_util.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@RunWith(Enclosed.class)
public class TryWithTest {
  private static class CloseException extends Exception {}

  private static class CloseRuntimeException extends RuntimeException {}

  private static class ActionException extends RuntimeException {}

  interface CheckableAutoCloseable extends AutoCloseable {
    boolean wasClosed();
  }

  private static class NoopCloseable implements CheckableAutoCloseable {
    boolean closed = false;

    @Override
    public void close() throws Exception {
      closed = true;
    }

    @Override
    public boolean wasClosed() {
      return closed;
    }
  }

  private static class ErrorCloseable implements CheckableAutoCloseable {
    boolean closed = false;

    @Override
    public void close() throws Exception {
      closed = true;
      throw new CloseException();
    }

    @Override
    public boolean wasClosed() {
      return closed;
    }
  }

  interface CheckableAsyncCloseable extends AsyncCloseable {
    boolean wasClosed();
  }

  private static class NoopAsyncCloseable implements CheckableAsyncCloseable {
    boolean closed = false;

    @Override
    public CompletionStage<Void> close() {
      closed = true;
      return FutureSupport.voidFuture();
    }

    @Override
    public boolean wasClosed() {
      return closed;
    }
  }

  private static class ErrorAsyncCloseable implements CheckableAsyncCloseable {
    boolean closed = false;
    boolean synchronousException;

    ErrorAsyncCloseable(boolean synchronousException) {
      this.synchronousException = synchronousException;
    }

    @Override
    public CompletionStage<Void> close() {
      closed = true;
      if (synchronousException) {
        throw new CloseRuntimeException();
      }
      return FutureSupport.errorStage(new CloseException());
    }

    @Override
    public boolean wasClosed() {
      return closed;
    }
  }

  public static class TryWithAsyncCloseable {
    private static Function<CheckableAsyncCloseable, Integer> successAction = ig -> 0;
    private static Function<CheckableAsyncCloseable, Integer> errorAction =
        ig -> {
          throw new ActionException();
        };

    @Test
    public void testTryWithNoExceptions() {
      validate(new NoopAsyncCloseable(), successAction, null, null);
    }

    @Test
    public void testTryWithActionException() {
      validate(new NoopAsyncCloseable(), errorAction, ActionException.class, null);
    }

    @Test
    public void testTryWithCloseAsyncException() {
      validate(new ErrorAsyncCloseable(false), successAction, CloseException.class, null);
    }

    @Test
    public void testTryWithCloseSyncException() {
      validate(new ErrorAsyncCloseable(true), successAction, CloseRuntimeException.class, null);
    }

    @Test
    public void testTryWithBothExceptionsAsyncClose() {
      validate(
          new ErrorAsyncCloseable(false), errorAction, ActionException.class, CloseException.class);
    }

    @Test
    public void testTryWithBothExceptionsSyncClose() {
      validate(
          new ErrorAsyncCloseable(true),
          errorAction,
          ActionException.class,
          CloseRuntimeException.class);
    }

    private void validate(
        final CheckableAsyncCloseable closeable,
        final Function<CheckableAsyncCloseable, Integer> action,
        final Class<? extends Exception> expectedException,
        final Class<? extends Exception> suppressedException) {
      try {
        AsyncCloseable.tryWith(closeable, action).toCompletableFuture().join();
        Assert.assertTrue(
            "expected exception ", expectedException == null && suppressedException == null);
      } catch (CompletionException e) {
        Assert.assertNotNull("unexpected exception " + e.getMessage(), expectedException);
        Assert.assertTrue(
            "unexpected exception " + e.getCause().getMessage(),
            e.getCause().getClass().equals(expectedException));
        if (suppressedException != null) {
          Throwable[] suppresed = e.getCause().getSuppressed();
          Assert.assertTrue(suppresed.length == 1);
          Assert.assertTrue(suppresed[0].getClass().equals(suppressedException));
        }
      }
      Assert.assertTrue(closeable.wasClosed());
    }
  }

  public static class TryWithAutoCloseable {
    private static Function<CheckableAutoCloseable, Integer> successAction = ig -> 0;
    private static Function<CheckableAutoCloseable, Integer> errorAction =
        ig -> {
          throw new ActionException();
        };

    @Test
    public void testTryWithNoExceptions() {
      validate(new NoopCloseable(), successAction, null, null);
    }

    @Test
    public void testTryWithActionException() {
      validate(new NoopCloseable(), errorAction, ActionException.class, null);
    }

    @Test
    public void testTryWithCloseException() {
      validate(new ErrorCloseable(), successAction, CloseException.class, null);
    }

    @Test
    public void testTryWithBothExceptions() {
      validate(
          new ErrorCloseable(), errorAction, ActionException.class, CloseException.class);
    }

    private void validate(
        final CheckableAutoCloseable closeable,
        final Function<CheckableAutoCloseable, Integer> action,
        final Class<? extends Exception> expectedException,
        final Class<? extends Exception> suppressedException) {
      try {
        FutureSupport.tryWith(CompletableFuture.completedFuture(closeable), action).toCompletableFuture().join();
        Assert.assertTrue(
            "expected exception ", expectedException == null && suppressedException == null);
      } catch (CompletionException e) {
        Assert.assertNotNull("unexpected exception " + e.getMessage(), expectedException);
        Assert.assertTrue(
            "unexpected exception " + e.getCause().getMessage(),
            e.getCause().getClass().equals(expectedException));
        if (suppressedException != null) {
          Throwable[] suppresed = e.getCause().getSuppressed();
          Assert.assertTrue(suppresed.length == 1);
          Assert.assertTrue(suppresed[0].getClass().equals(suppressedException));
        }
      }
      Assert.assertTrue(closeable.wasClosed());
    }
  }

  public static class TryComposeWithAsyncCloseable {

    private static Function<CheckableAsyncCloseable, CompletionStage<Integer>> successAction =
        ig -> CompletableFuture.completedFuture(0);
    private static Function<CheckableAsyncCloseable, CompletionStage<Integer>> asyncErrorAction =
        ig -> FutureSupport.errorStage(new ActionException());
    private static Function<CheckableAsyncCloseable, CompletionStage<Integer>> syncErrorAction =
        ig -> {
          throw new ActionException();
        };

    @Test
    public void testTryComposeWithNoExceptions() {
      validate(new NoopAsyncCloseable(), successAction, null, null);
    }

    @Test
    public void testTryComposeWithSyncActionException() {
      validate(new NoopAsyncCloseable(), syncErrorAction, ActionException.class, null);
    }

    @Test
    public void testTryComposeWithAsyncActionException() {
      validate(new NoopAsyncCloseable(), asyncErrorAction, ActionException.class, null);
    }

    @Test
    public void testTryComposeWithAsyncCloseException() {
      validate(new ErrorAsyncCloseable(false), successAction, CloseException.class, null);
    }

    @Test
    public void testTryComposeWithSyncCloseException() {
      validate(new ErrorAsyncCloseable(true), successAction, CloseRuntimeException.class, null);
    }

    @Test
    public void testTryComposeWithSyncCloseExceptionSyncActionException() {
      validate(
          new ErrorAsyncCloseable(true),
          syncErrorAction,
          ActionException.class,
          CloseRuntimeException.class);
    }

    @Test
    public void testTryComposeWithSyncCloseExceptionAsyncActionException() {
      validate(
          new ErrorAsyncCloseable(true),
          asyncErrorAction,
          ActionException.class,
          CloseRuntimeException.class);
    }

    @Test
    public void testTryComposeWithAsyncCloseExceptionSyncActionException() {
      validate(
          new ErrorAsyncCloseable(false),
          syncErrorAction,
          ActionException.class,
          CloseException.class);
    }

    @Test
    public void testTryComposeWithAsyncCloseExceptionAsyncActionException() {
      validate(
          new ErrorAsyncCloseable(false),
          asyncErrorAction,
          ActionException.class,
          CloseException.class);
    }

    private void validate(
        final CheckableAsyncCloseable closeable,
        final Function<CheckableAsyncCloseable, CompletionStage<Integer>> action,
        final Class<? extends Exception> expectedException,
        final Class<? extends Exception> suppressedException) {
      try {
        AsyncCloseable.tryComposeWith(closeable, action).toCompletableFuture().join();
        Assert.assertTrue(
            "expected exception ", expectedException == null && suppressedException == null);
      } catch (CompletionException e) {
        Assert.assertNotNull("unexpected exception " + e.getMessage(), expectedException);
        Assert.assertTrue(
            "unexpected exception " + e.getCause().getMessage(),
            e.getCause().getClass().equals(expectedException));
        if (suppressedException != null) {
          Throwable[] suppresed = e.getCause().getSuppressed();
          Assert.assertTrue(suppresed.length == 1);
          Assert.assertTrue(suppresed[0].getClass().equals(suppressedException));
        }
      }
      Assert.assertTrue(closeable.wasClosed());
    }
  }

  public static class TryComposeWithAutoCloseable {
    private static Function<CheckableAutoCloseable, CompletionStage<Integer>> successAction =
        ig -> CompletableFuture.completedFuture(0);
    private static Function<CheckableAutoCloseable, CompletionStage<Integer>> asyncErrorAction =
        ig -> FutureSupport.errorStage(new ActionException());
    private static Function<CheckableAutoCloseable, CompletionStage<Integer>> syncErrorAction =
        ig -> {
          throw new ActionException();
        };

    @Test
    public void testTryComposeWithNoExceptions() {
      validate(new NoopCloseable(), successAction, null, null);
    }

    @Test
    public void testTryComposeWithSyncActionException() {
      validate(new NoopCloseable(), syncErrorAction, ActionException.class, null);
    }

    @Test
    public void testTryComposeWithAsyncActionException() {
      validate(new NoopCloseable(), asyncErrorAction, ActionException.class, null);
    }

    @Test
    public void testTryComposeWithCloseException() {
      validate(new ErrorCloseable(), successAction, CloseException.class, null);
    }

    @Test
    public void testTryComposeWithCloseExceptionSyncActionException() {
      validate(new ErrorCloseable(), syncErrorAction, ActionException.class, CloseException.class);
    }

    @Test
    public void testTryComposeWithCloseExceptionAsyncActionException() {
      validate(new ErrorCloseable(), asyncErrorAction, ActionException.class, CloseException.class);
    }

    private void validate(
        final CheckableAutoCloseable closeable,
        Function<CheckableAutoCloseable, CompletionStage<Integer>> action,
        final Class<? extends Exception> expectedException,
        final Class<? extends Exception> suppressedException) {
      try {
        FutureSupport.tryComposeWith(CompletableFuture.completedFuture(closeable), action)
            .toCompletableFuture()
            .join();
        Assert.assertTrue(
            "expected exception ", expectedException == null && suppressedException == null);
      } catch (CompletionException e) {

        Assert.assertNotNull("unexpected exception " + e.getMessage(), expectedException);
        Assert.assertTrue(
            "unexpected exception " + e.getCause().getMessage(),
            e.getCause().getClass().equals(expectedException));
        if (suppressedException != null) {
          Throwable[] suppresed = e.getCause().getSuppressed();
          Assert.assertTrue(suppresed.length == 1);
          Assert.assertTrue(suppresed[0].getClass().equals(suppressedException));
        }
      }
      Assert.assertTrue(closeable.wasClosed());
    }
  }
}
