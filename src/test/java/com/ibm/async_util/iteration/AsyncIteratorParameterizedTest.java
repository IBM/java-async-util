package com.ibm.async_util.iteration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.ibm.async_util.util.Either;
import com.ibm.async_util.util.FutureSupport;

@RunWith(Enclosed.class)
public class AsyncIteratorParameterizedTest {
  private static class TestException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  static final TestException testException = new TestException();

  static final List<Function<AsyncIterator<?>, CompletionStage<?>>> terminalMethods =
      // for testing convenience every terminal operation should consume at least 3 results
      Arrays.asList(
          new Function<AsyncIterator<?>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<?> it) {
              return it.fold((i, j) -> new Object(), new Object());
            }

            @Override
            public String toString() {
              return "fold";
            }
          },
          new Function<AsyncIterator<?>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<?> it) {
              return it.consume();
            }

            @Override
            public String toString() {
              return "consume";
            }
          },
          new Function<AsyncIterator<?>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<?> it) {
              return it.collect(Collectors.toList());
            }

            @Override
            public String toString() {
              return "collect1";
            }
          },
          new Function<AsyncIterator<?>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<?> it) {
              return it.collect(ArrayList<Object>::new, List::add);
            }

            @Override
            public String toString() {
              return "collect2";
            }
          },
          new Function<AsyncIterator<?>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<?> it) {
              return it.forEach(i -> {
              });
            }

            @Override
            public String toString() {
              return "forEach";
            }
          },
          new Function<AsyncIterator<?>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<?> it) {
              final AtomicInteger calls = new AtomicInteger();
              return it.find(
                  i -> {
                    return calls.getAndIncrement() == 3;
                  });
            }

            @Override
            public String toString() {
              return "find";
            }
          });

  static final List<Function<AsyncIterator<Integer>, CompletionStage<?>>> exceptionalTerminalMethods =
      Arrays.asList(
          new Function<AsyncIterator<Integer>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<Integer> it) {
              return it.fold(
                  (i, j) -> {
                    throw testException;
                  },
                  0);
            }

            @Override
            public String toString() {
              return "fold";
            }
          },
          new Function<AsyncIterator<Integer>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<Integer> it) {
              return it.collect(
                  new Collector<Integer, Object, Object>() {
                    @Override
                    public Supplier<Object> supplier() {
                      throw testException;
                    }

                    @Override
                    public BiConsumer<Object, Integer> accumulator() {
                      return null;
                    }

                    @Override
                    public BinaryOperator<Object> combiner() {
                      return null;
                    }

                    @Override
                    public Function<Object, Object> finisher() {
                      return null;
                    }

                    @Override
                    public Set<Characteristics> characteristics() {
                      return null;
                    }
                  });
            }

            @Override
            public String toString() {
              return "collect(supply-error)";
            }
          },
          new Function<AsyncIterator<Integer>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<Integer> it) {
              return it.collect(
                  new Collector<Integer, Object, Object>() {
                    @Override
                    public Supplier<Object> supplier() {
                      return Object::new;
                    }

                    @Override
                    public BiConsumer<Object, Integer> accumulator() {
                      return (o, i) -> {
                        throw testException;
                      };
                    }

                    @Override
                    public BinaryOperator<Object> combiner() {
                      return null;
                    }

                    @Override
                    public Function<Object, Object> finisher() {
                      return null;
                    }

                    @Override
                    public Set<Characteristics> characteristics() {
                      return null;
                    }
                  });
            }

            @Override
            public String toString() {
              return "collect(accumulate-error)";
            }
          },
          new Function<AsyncIterator<Integer>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<Integer> it) {
              return it.collect(
                  ArrayList<Integer>::new,
                  (acc, t) -> {
                    throw testException;
                  });
            }

            @Override
            public String toString() {
              return "collect(accumulate-error-2)";
            }
          },
          new Function<AsyncIterator<Integer>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<Integer> it) {
              return it.forEach(
                  i -> {
                    throw testException;
                  });
            }

            @Override
            public String toString() {
              return "forEach";
            }
          },
          new Function<AsyncIterator<Integer>, CompletionStage<?>>() {
            @Override
            public CompletionStage<?> apply(final AsyncIterator<Integer> it) {
              return it.find(
                  i -> {
                    throw testException;
                  });
            }

            @Override
            public String toString() {
              return "find";
            }
          });

  static final List<Function<AsyncIterator<Integer>, AsyncIterator<?>>> intermediateMethods =
      Arrays.asList(
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.thenApply(i -> i);
            }

            @Override
            public String toString() {
              return "thenApply";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.thenApplyAsync(i -> i);
            }

            @Override
            public String toString() {
              return "thenApplyAsync";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.thenCompose(
                  i -> FutureSupport.completedStage(i));
            }

            @Override
            public String toString() {
              return "thenCompose";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.thenComposeAsync(
                  i -> FutureSupport.completedStage(i));
            }

            @Override
            public String toString() {
              return "thenComposeAsync";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.thenFlatten(i -> AsyncIterator.<Integer>once(i));
            }

            @Override
            public String toString() {
              return "thenFlatten";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.filter(i -> true);
            }

            @Override
            public String toString() {
              return "filter";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.filterApply(Optional::of);
            }

            @Override
            public String toString() {
              return "filterApply";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.filterCompose(
                  i -> FutureSupport.completedStage(Optional.of(i)));
            }

            @Override
            public String toString() {
              return "filterCompose";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.take(5);
            }

            @Override
            public String toString() {
              return "take";
            }
          },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.takeWhile(i -> true);
            }

            @Override
            public String toString() {
              return "takeWhile";
            }
          },
          // new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
          // @Override
          // public AsyncIterator<?> apply(AsyncIterator<Integer> it) {
          // return it.batch(Collectors.toList(), 1);
          // }
          //
          // @Override
          // public String toString() {
          // return "batch";
          // }
          // },
          new Function<AsyncIterator<Integer>, AsyncIterator<?>>() {
            @Override
            public AsyncIterator<?> apply(final AsyncIterator<Integer> it) {
              return it.fuse();
            }

            @Override
            public String toString() {
              return "fuse";
            }
          });

  @RunWith(Parameterized.class)
  public static class PipelineTest {
    @Parameterized.Parameter
    public Function<AsyncIterator<Integer>, AsyncIterator<?>> intermediate;

    @Parameterized.Parameter(1)
    public Function<AsyncIterator<?>, CompletionStage<?>> terminal;

    @Parameterized.Parameters(name = "{index} intermediate: {0}, terminal: {1}")
    public static Collection<Object[]> data() {
      final List<Object[]> list = new ArrayList<>();
      for (final Object intermediate : intermediateMethods) {
        for (final Object terminal : terminalMethods) {
          list.add(new Object[] {intermediate, terminal});
        }
      }
      return list;
    }

    @Test
    public void testPipeline() {
      final AsyncIterator<Integer> ai = AsyncIterator.range(0, 10, 1);
      this.terminal.apply(this.intermediate.apply(ai)).toCompletableFuture().join();
    }

    @Test
    public void testEmptyPipeline() {
      this.terminal.apply(this.intermediate.apply(AsyncIterator.empty())).toCompletableFuture()
          .join();
    }

    @Test(expected = TestException.class)
    public void testExceptionalPipeline() throws Throwable {
      try {
        this.terminal
            .apply(this.intermediate.apply(AsyncIterator.error(testException)))
            .toCompletableFuture()
            .join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test(expected = TestException.class)
    public void testDelayedExceptionalPipeline() throws Throwable {
      try {

        final AsyncIterator<Integer> concat =
            AsyncIterator.concat(
                Arrays.asList(
                    AsyncIterator.repeat(0).take(3),
                    AsyncIterator.<Integer>error(testException),
                    AsyncIterator.repeat(1).take(3))
                    .iterator());
        this.terminal
            .apply(this.intermediate.apply(concat))
            .toCompletableFuture()
            .join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test(expected = TestException.class)
    public void testExceptionalPipelineShortcircuit() throws Throwable {
      try {

        final AsyncIterator<Integer> concat =
            AsyncIterator.concat(
                Arrays.asList(
                    AsyncIterator.repeat(0).take(3),
                    AsyncIterator.<Integer>error(testException),
                    AsyncIterator.repeat(1)) // infinite
                    .iterator());
        this.terminal.apply(this.intermediate.apply(concat)).toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test
    public void testClosePipeline() {
      final AtomicBoolean closed = new AtomicBoolean();
      final AsyncIterator<Integer> ai =
          new AsyncIterator<Integer>() {
            @Override
            public CompletionStage<Either<End, Integer>> nextFuture() {
              return FutureSupport.completedStage(Either.right(1));
            }

            @Override
            public CompletionStage<Void> close() {
              closed.set(true);
              return FutureSupport.voidFuture();
            }
          }.take(10);
      final AsyncIterator<?> intermediateAi = this.intermediate.apply(ai);
      this.terminal.apply(intermediateAi).toCompletableFuture().join();
      Assert.assertFalse(closed.get());
      intermediateAi.close().toCompletableFuture().join();
      Assert.assertTrue(closed.get());
    }

    @Test
    public void testCloseAfterExceptionalPipeline() {
      final AtomicBoolean closed = new AtomicBoolean();
      final AsyncIterator<Integer> ai =
          new AsyncIterator<Integer>() {
            @Override
            public CompletionStage<Either<End, Integer>> nextFuture() {
              throw testException;
            }

            @Override
            public CompletionStage<Void> close() {
              closed.set(true);
              return FutureSupport.voidFuture();
            }
          }.take(10);
      final AsyncIterator<?> intermediateAi = this.intermediate.apply(ai);
      try {
        this.terminal.apply(intermediateAi).toCompletableFuture().join();
      } catch (final CompletionException e) {
        Assert.assertTrue(e.getCause() instanceof TestException);
      }
      Assert.assertFalse(closed.get());
      intermediateAi.close().toCompletableFuture().join();
      Assert.assertTrue(closed.get());
    }
  }

  @RunWith(Parameterized.class)
  public static class IntermediateTest {

    @Parameterized.Parameter
    public Function<AsyncIterator<Integer>, AsyncIterator<?>> fn;

    @Parameterized.Parameters(name = "{index} intermediate: {0}")
    public static Collection<Object[]> data() {
      return intermediateMethods.stream().map(fn -> new Object[] {fn}).collect(Collectors.toList());
    }

    @Test
    public void testLazy() {
      final AtomicInteger i = new AtomicInteger();
      final AsyncIterator<Integer> it =
          () -> FutureSupport.completedStage(Either.right(i.incrementAndGet()));
      final AsyncIterator<?> intermediate = this.fn.apply(it);
      Assert.assertEquals(0, i.get());
      intermediate.nextFuture().toCompletableFuture().join();
      Assert.assertEquals(1, i.get());
      intermediate.nextFuture().toCompletableFuture().join();
      Assert.assertEquals(2, i.get());
    }

    @Test(expected = TestException.class)
    public void testExceptionPropagation() throws Throwable {
      try {
        final AsyncIterator<Integer> concat =
            AsyncIterator.concat(
                Arrays.asList(
                    AsyncIterator.repeat(0).take(3),
                    AsyncIterator.<Integer>error(testException),
                    AsyncIterator.repeat(1).take(3))
                    .iterator());
        this.fn.apply(concat).consume().toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test
    public void testClosePropagation() {
      final AtomicBoolean closed = new AtomicBoolean();
      final AsyncIterator<Integer> ai =
          new AsyncIterator<Integer>() {
            @Override
            public CompletionStage<Either<End, Integer>> nextFuture() {
              return FutureSupport.completedStage(Either.right(1));
            }

            @Override
            public CompletionStage<Void> close() {
              closed.set(true);
              return FutureSupport.voidFuture();
            }
          }.take(10);
      final AsyncIterator<?> it2 = this.fn.apply(ai);
      it2.consume().toCompletableFuture().join();
      Assert.assertFalse(closed.get());
      it2.close().toCompletableFuture().join();
      Assert.assertTrue(closed.get());
    }

    @Test(expected = TestException.class)
    public void testExceptionalCloseTest() throws Throwable {
      final AsyncIterator<Integer> ai =
          new AsyncIterator<Integer>() {
            @Override
            public CompletionStage<Either<End, Integer>> nextFuture() {
              return FutureSupport.completedStage(Either.right(1));
            }

            @Override
            public CompletionStage<Void> close() {
              throw testException;
            }
          };
      final AsyncIterator<?> it2 = this.fn.apply(ai);
      try {
        it2.nextFuture().toCompletableFuture().join();
      } catch (final Exception e) {
        Assert.fail(e.getMessage());
      }
      try {
        it2.close().toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test
    public void testRecoverAfterException() {
      final AsyncIterator<Integer> it =
          AsyncIterator.concat(
              Arrays.asList(
                  AsyncIterators.<Integer>errorOnce(testException),
                  AsyncIterator.range(1, 5, 1))
                  .iterator());
      final AsyncIterator<?> it2 = this.fn.apply(it);
      try {
        it2.nextFuture().toCompletableFuture().join();
        Assert.fail("expected exception");
      } catch (final CompletionException e) {
        // expected
      }
      it2.nextFuture().toCompletableFuture().join();
      it2.consume().toCompletableFuture().join();
      it2.close().toCompletableFuture().join();
    }
  }

  @RunWith(Parameterized.class)
  public static class ExceptionThrowingTerminalOperationTest {

    @Parameterized.Parameter
    public Function<AsyncIterator<Integer>, CompletionStage<?>> fn;

    @Parameterized.Parameters(name = "{index} terminal: {0}")
    public static Collection<Object[]> data() {
      return exceptionalTerminalMethods
          .stream()
          .map(fn -> new Object[] {fn})
          .collect(Collectors.toList());
    }

    @Test(expected = TestException.class)
    public void testErrorTerminal() throws Throwable {
      try {
        this.fn.apply(AsyncIterator.repeat(0).take(5)).toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test(expected = TestException.class)
    public void testErrorTerminalShortCircuit() throws Throwable {
      try {
        this.fn.apply(AsyncIterator.repeat(0)).toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TerminalTest {
    @Parameterized.Parameters(name = "{index} terminal: {0}")
    public static Collection<Object[]> data() {
      return terminalMethods.stream().map(fn -> new Object[] {fn}).collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public Function<AsyncIterator<Integer>, CompletionStage<?>> fn;

    @Test
    public void testEmpty() {
      this.fn.apply(AsyncIterator.empty()).toCompletableFuture().join();
    }

    @Test(expected = TestException.class)
    public void testImmediateException() throws Throwable {
      try {
        this.fn.apply(AsyncIterator.error(testException)).toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test(expected = TestException.class)
    public void testDelayedException() throws Throwable {
      try {
        final AsyncIterator<Integer> concat =
            AsyncIterator.concat(
                Arrays.asList(
                    AsyncIterator.repeat(0).take(3),
                    AsyncIterator.<Integer>error(testException),
                    AsyncIterator.repeat(1).take(3))
                    .iterator());
        this.fn.apply(concat).toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }

    @Test(expected = TestException.class)
    public void testExceptionShortCircuit() throws Throwable {
      try {
        final AsyncIterator<Integer> concat =
            AsyncIterator.concat(
                Arrays.asList(
                    AsyncIterator.repeat(0).take(3),
                    AsyncIterator.<Integer>error(testException),
                    AsyncIterator.repeat(1)) // infinite
                    .iterator());
        this.fn.apply(concat).toCompletableFuture().join();
      } catch (final CompletionException e) {
        throw e.getCause();
      }
    }
  }
}
