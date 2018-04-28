/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.asyncutil.locks.AsyncEpoch.EpochToken;
import com.ibm.asyncutil.util.Combinators;
import com.ibm.asyncutil.util.TestUtil;

public abstract class AbstractAsyncEpochTest {
  abstract AsyncEpoch newEpoch();

  @Test
  public void testBlockOnActors() {
    final AsyncEpoch e = newEpoch();

    final AsyncEpoch.EpochToken t1 = e.enter().orElseThrow(AssertionError::new);
    Assert.assertFalse(e.isTerminated());

    final AtomicReference<Boolean> firstTerminate = new AtomicReference<>(null);
    e.terminate().thenAccept(wasFirst -> firstTerminate.set(wasFirst));

    Assert.assertTrue(e.isTerminated());
    Assert.assertFalse(e.enter().isPresent());
    final AtomicReference<Boolean> secondTerminate = new AtomicReference<>(null);
    e.terminate().thenAccept(wasFirst -> secondTerminate.set(wasFirst));

    Assert.assertNull(firstTerminate.get());
    Assert.assertNull(secondTerminate.get());

    t1.close();
    Assert.assertTrue(firstTerminate.get());
    Assert.assertFalse(secondTerminate.get());
  }

  @Test
  public void testManyEntrants() {
    final AsyncEpoch e = newEpoch();

    final int count = 100_000;
    final AsyncEpoch.EpochToken[] tokens = IntStream.range(0, count).parallel()
        .mapToObj(ignored -> e.enter().get()).toArray(AsyncEpoch.EpochToken[]::new);

    final AtomicBoolean completed = new AtomicBoolean(false);
    e.terminate().thenAccept(ignored -> completed.set(true));

    // terminate all but 1
    Arrays.stream(tokens, 1, tokens.length).parallel().forEach(AsyncEpoch.EpochToken::close);
    Assert.assertTrue(e.isTerminated());
    Assert.assertFalse(completed.get());

    tokens[0].close();
    Assert.assertTrue(completed.get());
  }

  @Test(expected = IllegalStateException.class)
  public void testExceptionOnExcessClose() {
    final AsyncEpoch e = newEpoch();
    final AsyncEpoch.EpochToken t = e.enter().orElseThrow(AssertionError::new);
    t.close();

    // incorrect extra close
    t.close();

    // some implementations won't detect close inconsistency until terminate
    e.terminate().toCompletableFuture().join();
  }

  @Test
  public void testTerminateWithoutEntrants() {
    // never any entrants
    {
      final AsyncEpoch e = newEpoch();
      Assert.assertFalse(e.isTerminated());
      Assert.assertTrue(e.terminate().toCompletableFuture().isDone());
      Assert.assertTrue(e.isTerminated());
      Assert.assertFalse(e.enter().isPresent());
      Assert.assertFalse(TestUtil.join(e.terminate()));
    }
    // all entrants exited
    {
      final AsyncEpoch e = newEpoch();
      Assert.assertFalse(e.isTerminated());
      final AsyncEpoch.EpochToken t = e.enter().orElseThrow(AssertionError::new);
      e.enter().orElseThrow(AssertionError::new).close();
      t.close();
      Assert.assertFalse(e.isTerminated());

      Assert.assertTrue(e.terminate().toCompletableFuture().isDone());
      Assert.assertTrue(e.isTerminated());
      Assert.assertFalse(e.enter().isPresent());
      Assert.assertFalse(TestUtil.join(e.terminate()));
    }
  }

  @Test
  public void testAwaitCompletion() {
    {
      final AsyncEpoch e = newEpoch();
      final CompletableFuture<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      TestUtil.join(e.terminate());
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(e.awaitCompletion().toCompletableFuture().isDone());
    }

    {
      final AsyncEpoch e = newEpoch();
      final CompletableFuture<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      final AsyncEpoch.EpochToken token1 = e.enter().orElseThrow(AssertionError::new);
      Assert.assertFalse(f.isDone());
      Assert.assertFalse(e.awaitCompletion().toCompletableFuture().isDone());
      e.terminate();
      Assert.assertFalse(f.isDone());
      Assert.assertFalse(e.awaitCompletion().toCompletableFuture().isDone());
      token1.close();
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(e.awaitCompletion().toCompletableFuture().isDone());
    }

    {
      final AsyncEpoch e = newEpoch();
      final AsyncEpoch.EpochToken token1 = e.enter().orElseThrow(AssertionError::new);
      final CompletableFuture<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      final AsyncEpoch.EpochToken token2 = e.enter().orElseThrow(AssertionError::new);
      Assert.assertFalse(f.isDone());
      Assert.assertFalse(e.awaitCompletion().toCompletableFuture().isDone());
      e.terminate();
      Assert.assertFalse(f.isDone());
      Assert.assertFalse(e.awaitCompletion().toCompletableFuture().isDone());
      token2.close();
      Assert.assertFalse(f.isDone());
      Assert.assertFalse(e.awaitCompletion().toCompletableFuture().isDone());
      token1.close();
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(e.awaitCompletion().toCompletableFuture().isDone());
    }

    {
      final AsyncEpoch e = newEpoch();
      TestUtil.join(e.terminate());
      final Future<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(e.awaitCompletion().toCompletableFuture().isDone());
    }
  }

  @Test
  public void newTerminatedEpoch() {
    final AsyncEpoch epoch = AsyncEpoch.newTerminatedEpoch();
    final String msg = "Expected epoch to already be termianted";
    Assert.assertTrue(msg, epoch.isTerminated());
    Assert.assertFalse(msg, epoch.enter().isPresent());
    Assert.assertFalse(msg, TestUtil.join(epoch.terminate()));
    Assert.assertFalse(msg, TestUtil.join(epoch.terminate()));
    Assert.assertFalse(msg, epoch.enter().isPresent());
  }

  @Test
  public void testManyTerminates() {
    final AsyncEpoch e = newEpoch();
    final int count = 100_000;
    final EpochToken[] tokens = IntStream.range(0, count).parallel()
        .mapToObj(ignored -> e.enter().get())
        .toArray(EpochToken[]::new);

    // close all
    Arrays.stream(tokens, 0, tokens.length).parallel().forEach(EpochToken::close);
    Assert.assertFalse(e.isTerminated());

    // count terminates concurrently terminating
    final List<CompletionStage<Boolean>> terminates = IntStream.range(0, count).parallel()
        .mapToObj(ignored -> e.terminate())
        .collect(Collectors.toList());
    Assert.assertTrue(e.isTerminated());

    final CompletableFuture<Collection<Boolean>> allDone =
        Combinators.collect(terminates).toCompletableFuture();
    Assert.assertTrue(allDone.isDone());

    Assert.assertEquals(1, allDone.join().stream().filter(b -> b).count());
  }

  @Test
  public void testConcurrentEnterCloseTerminate()
      throws InterruptedException, ExecutionException, TimeoutException {
    final int closeAfter = 10_000;
    final AsyncEpoch e = newEpoch();

    final ConcurrentLinkedQueue<CompletableFuture<Void>> closes = new ConcurrentLinkedQueue<>();
    final List<CompletableFuture<Void>> stages =
        IntStream.range(0, ForkJoinPool.getCommonPoolParallelism())
            .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
              int numEnters = 0;
              while (true) {
                if (i == 0 && numEnters++ == closeAfter) {
                  return e.terminate().thenApply(terminated -> {
                    Assert.assertTrue(terminated);
                    Assert.assertTrue(e.isTerminated());
                    return (Void) null;
                  });
                }
                final Optional<EpochToken> token = e.enter();
                if (token.isPresent()) {
                  closes.add(CompletableFuture.runAsync(() -> token.get().close()));
                } else {
                  return CompletableFuture.<Void>completedFuture(null);
                }
              }
            }).thenCompose(stage -> stage)).collect(Collectors.toList());
    Combinators.allOf(stages).toCompletableFuture().get(10, TimeUnit.SECONDS);

    // all closes should have been scheduled
    Combinators.allOf(closes).toCompletableFuture().get(10, TimeUnit.SECONDS);

    Assert.assertTrue(e.isTerminated());
  }

  @Test
  public void testThreadBiasedClose() {
    {
      final AsyncEpoch e = newEpoch();

      final int count = 100_000;
      final EpochToken[] tokens = IntStream.range(0, count).parallel()
          .mapToObj(ignored -> e.enter().get())
          .toArray(EpochToken[]::new);

      // close all tokens on one thread, then terminate
      for (final EpochToken token : tokens) {
        token.close();
      }
      final CompletableFuture<Boolean> f = e.terminate().toCompletableFuture();
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(f.join());

    }

    {
      final AsyncEpoch e = newEpoch();

      final int count = 100_000;
      final EpochToken[] tokens = IntStream.range(0, count).parallel()
          .mapToObj(ignored -> e.enter().get())
          .toArray(EpochToken[]::new);

      // terminate competes with more distributed enters
      final CompletableFuture<Void> closes = CompletableFuture.runAsync(() -> {
        for (final EpochToken token : tokens) {
          token.close();
        }
      });
      final EpochToken[] tokens2 = IntStream.range(0, count).parallel()
          .mapToObj(ignored -> e.enter().get())
          .toArray(EpochToken[]::new);

      closes.join();
      final CompletableFuture<Boolean> f = e.terminate().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      for (final EpochToken token : tokens2) {
        token.close();
      }
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(f.join());
    }
  }
}
