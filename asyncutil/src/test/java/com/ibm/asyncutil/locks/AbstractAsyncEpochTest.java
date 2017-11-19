/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

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
    t.close();
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
}
