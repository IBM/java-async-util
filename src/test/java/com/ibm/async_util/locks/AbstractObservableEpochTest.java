//
// (C) Copyright IBM Corp. 2005 All Rights Reserved.
//
// Contact Information:
//
// IBM Corporation
// Legal Department
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// END-OF-HEADER
//
// -----------------------
// @author: renar
//
// Date: Apr 22, 2016
// ---------------------

package com.ibm.async_util.locks;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import com.ibm.async_util.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractObservableEpochTest {
  abstract ObservableEpoch newEpoch();

  @Test
  public void testBlockOnActors() {
    final ObservableEpoch e = newEpoch();

    final ObservableEpoch.EpochToken t1 = e.enter().orElseThrow(AssertionError::new);
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
    final ObservableEpoch e = newEpoch();

    final int count = 100_000;
    final ObservableEpoch.EpochToken[] tokens = IntStream.range(0, count).parallel()
        .mapToObj(ignored -> e.enter().get()).toArray(ObservableEpoch.EpochToken[]::new);

    final AtomicBoolean completed = new AtomicBoolean(false);
    e.terminate().thenAccept(ignored -> completed.set(true));

    // terminate all but 1
    Arrays.stream(tokens, 1, tokens.length).parallel().forEach(ObservableEpoch.EpochToken::close);
    Assert.assertTrue(e.isTerminated());
    Assert.assertFalse(completed.get());

    tokens[0].close();
    Assert.assertTrue(completed.get());
  }

  @Test(expected = IllegalStateException.class)
  public void testExceptionOnExcessClose() {
    final ObservableEpoch e = newEpoch();
    final ObservableEpoch.EpochToken t = e.enter().orElseThrow(AssertionError::new);
    t.close();
    t.close();
  }

  @Test
  public void testTerminateWithoutEntrants() {
    // never any entrants
    {
      final ObservableEpoch e = newEpoch();
      Assert.assertFalse(e.isTerminated());
      Assert.assertTrue(e.terminate().toCompletableFuture().isDone());
      Assert.assertTrue(e.isTerminated());
      Assert.assertFalse(e.enter().isPresent());
      Assert.assertFalse(TestUtil.join(e.terminate()));
    }
    // all entrants exited
    {
      final ObservableEpoch e = newEpoch();
      Assert.assertFalse(e.isTerminated());
      final ObservableEpoch.EpochToken t = e.enter().orElseThrow(AssertionError::new);
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
      final ObservableEpoch e = newEpoch();
      final CompletableFuture<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      TestUtil.join(e.terminate());
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(e.awaitCompletion().toCompletableFuture().isDone());
    }

    {
      final ObservableEpoch e = newEpoch();
      final CompletableFuture<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      final ObservableEpoch.EpochToken token1 = e.enter().orElseThrow(AssertionError::new);
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
      final ObservableEpoch e = newEpoch();
      final ObservableEpoch.EpochToken token1 = e.enter().orElseThrow(AssertionError::new);
      final CompletableFuture<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertFalse(f.isDone());
      final ObservableEpoch.EpochToken token2 = e.enter().orElseThrow(AssertionError::new);
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
      final ObservableEpoch e = newEpoch();
      TestUtil.join(e.terminate());
      final Future<Void> f = e.awaitCompletion().toCompletableFuture();
      Assert.assertTrue(f.isDone());
      Assert.assertTrue(e.awaitCompletion().toCompletableFuture().isDone());
    }
  }

  @Test
  public void newTerminatedEpoch() {
    final ObservableEpoch epoch = ObservableEpoch.newTerminatedEpoch();
    final String msg = "Expected epoch to already be termianted";
    Assert.assertTrue(msg, epoch.isTerminated());
    Assert.assertFalse(msg, epoch.enter().isPresent());
    Assert.assertFalse(msg, TestUtil.join(epoch.terminate()));
    Assert.assertFalse(msg, TestUtil.join(epoch.terminate()));
    Assert.assertFalse(msg, epoch.enter().isPresent());
  }
}
