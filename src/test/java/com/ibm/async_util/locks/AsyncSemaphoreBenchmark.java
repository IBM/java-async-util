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
// Date: Apr 23, 2016
// ---------------------

package com.ibm.async_util.locks;

import java.io.IOException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import com.ibm.async_util.util.FutureSupport;
import com.ibm.async_util.util.TestUtil;

public final class AsyncSemaphoreBenchmark {
  private AsyncSemaphoreBenchmark() {}

  @Fork(1)
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
  public static abstract class AbstractPaired {
    AsyncSemaphore semaphore;

    @Param({"fair", "simple", "sync"})
    String impl;

    private final int threadCount;

    public AbstractPaired(final int threadCount) {
      this.threadCount = threadCount;
    }

    @Setup
    public void setupBenchmark() {
      final long initPermits = this.threadCount;
      AsyncSemaphoreBenchmark.implAcquireInitCheck(this.impl, initPermits);
      this.semaphore = AsyncSemaphoreBenchmark.getImpl(this.impl, initPermits);
    }

    @Benchmark
    public void acquireReleaseNoWait() {
      this.semaphore.acquire();
      Blackhole.consumeCPU(40);
      this.semaphore.release();
      Blackhole.consumeCPU(40);
    }

    @Benchmark
    public void acquireRelease() {
      TestUtil.join(this.semaphore.acquire()
          .thenAccept(ignored -> {
            Blackhole.consumeCPU(40);
            this.semaphore.release();
          }));
      Blackhole.consumeCPU(40);
    }
  }

  @Threads(1)
  public static class Paired1Thread extends AbstractPaired {
    public Paired1Thread() {
      super(1);
    }
  }

  @Threads(4)
  public static class Paired4Thread extends AbstractPaired {
    public Paired4Thread() {
      super(4);
    }
  }

  @Threads(8)
  public static class Paired8Thread extends AbstractPaired {
    public Paired8Thread() {
      super(8);
    }
  }


  @Fork(1)
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public static abstract class AbstractIterationBenchmark {
    @Param({"fair", "simple", "sync"})
    String impl;

    protected AsyncSemaphore semaphore;

    protected void setupIteration(final int permits) {
      this.semaphore = AsyncSemaphoreBenchmark.getImpl(this.impl, permits);
      System.gc();
    }
  }

  public abstract static class AbstractAllAcquire extends AbstractIterationBenchmark {
    @Param({"0", Integer.MAX_VALUE + ""})
    int initPermits;

    @Setup(Level.Iteration)
    public void setupIteration() {
      AsyncSemaphoreBenchmark.implAcquireInitCheck(this.impl, this.initPermits);
      super.setupIteration(this.initPermits);
    }

    @Benchmark
    public CompletionStage<Void> acquire() {
      return this.semaphore.acquire();
    }
  }

  @Threads(1)
  @Warmup(iterations = 3000, batchSize = 10_000)
  @Measurement(iterations = 1500, batchSize = 10_000)
  public static class AllAcquire1Thread10k extends AbstractAllAcquire {
  }

  @Threads(4)
  @Warmup(iterations = 3000, batchSize = 1000)
  @Measurement(iterations = 1500, batchSize = 1000)
  public static class AllAcquire4Thread1k extends AbstractAllAcquire {
  }

  @Threads(8)
  @Warmup(iterations = 3000, batchSize = 1000)
  @Measurement(iterations = 1500, batchSize = 1000)
  public static class AllAcquire8Thread1K extends AbstractAllAcquire {
  }

  public abstract static class AbstractAllRelease extends AbstractIterationBenchmark {
    abstract int getBatchParam();

    @Setup(Level.Iteration)
    public void setupIteration() {
      final int batch = getBatchParam();
      super.setupIteration(0);
      if (batch > 0) {
        // initialize semaphore to zero, but then acquire to create a deficit
        // release will restore waiters
        AsyncSemaphoreBenchmark.implAcquireInitCheck(this.impl, 0);
        for (int i = 0; i < batch; i++) {
          this.semaphore.acquire();
        }
      }
      // else
      // initialize semaphore to zero, release freely (no waiters, idle permits)
    }

    @Benchmark
    public void release() {
      this.semaphore.release();
    }
  }

  private static final int RELEASE_BATCH_1THREAD = 10_000;

  @Threads(1)
  @Warmup(iterations = 3000, batchSize = RELEASE_BATCH_1THREAD)
  @Measurement(iterations = 1500, batchSize = RELEASE_BATCH_1THREAD)
  public static class AllRelease1Thread extends AbstractAllRelease {
    @Param({"0", RELEASE_BATCH_1THREAD + ""})
    int batchParam;

    @Override
    int getBatchParam() {
      return this.batchParam;
    }
  }

  private static final int RELEASE_BATCH_4THREAD = 5_000;

  @Threads(4)
  @Warmup(iterations = 3000, batchSize = RELEASE_BATCH_4THREAD)
  @Measurement(iterations = 1500, batchSize = RELEASE_BATCH_4THREAD)
  public static class AllRelease4Thread extends AbstractAllRelease {
    @Param({"0", RELEASE_BATCH_4THREAD + ""})
    int batchParam;

    @Override
    int getBatchParam() {
      return this.batchParam;
    }
  }

  private static final int RELEASE_BATCH_8THREAD = 3_000;

  @Threads(8)
  @Warmup(iterations = 3000, batchSize = RELEASE_BATCH_8THREAD)
  @Measurement(iterations = 1500, batchSize = RELEASE_BATCH_8THREAD)
  public static class AllRelease8Thread extends AbstractAllRelease {
    @Param({"0", RELEASE_BATCH_8THREAD + ""})
    int batchParam;

    @Override
    int getBatchParam() {
      return this.batchParam;
    }
  }

  private static void implAcquireInitCheck(final String impl, final long permits) {
    if (impl.equals("sync") && permits <= 0) {
      throw new UnsupportedOperationException(
          String.format("test cannot run for configuration (%s, %d), proceed to next test", impl,
              permits));
    }
  }

  private static AsyncSemaphore getImpl(final String impl, final long permits) {
    switch (impl) {
      case "fair":
        return new WrappedFairAsyncSemaphore(permits);
      case "sync":
        return new WrappedSynchronousSemaphore(permits);
      default:
        throw new IllegalArgumentException("unknown semaphore implementation:" + impl);
    }
  }

  private static class WrappedFairAsyncSemaphore implements AsyncSemaphore {
    /*
     * include a delegate wrapper in order not to favor this class for directly implementing the
     * interface
     */
    private final FairAsyncSemaphore semaphore;

    public WrappedFairAsyncSemaphore(final long permits) {
      this.semaphore = new FairAsyncSemaphore(permits);
    }

    @Override
    public final CompletionStage<Void> acquire(final long permits) {
      return this.semaphore.acquire(permits);
    }

    @Override
    public final void release(final long permits) {
      this.semaphore.release(permits);
    }

    @Override
    public final boolean tryAcquire(final long permits) {
      return this.semaphore.tryAcquire(permits);
    }

    @Override
    public final long drainPermits() {
      return this.semaphore.drainPermits();
    }

    @Override
    public final long getAvailablePermits() {
      return this.semaphore.getAvailablePermits();
    }

    @Override
    public int getQueueLength() {
      return this.semaphore.getQueueLength();
    }
  }

  private static class WrappedSynchronousSemaphore implements AsyncSemaphore {
    private final Semaphore semaphore;

    public WrappedSynchronousSemaphore(final long permits) {
      this.semaphore = new Semaphore(Math.toIntExact(permits));
    }

    @Override
    public CompletionStage<Void> acquire(final long permits) {
      this.semaphore.acquireUninterruptibly(Math.toIntExact(permits));
      return FutureSupport.voidFuture();
    }

    @Override
    public void release(final long permits) {
      this.semaphore.release(Math.toIntExact(permits));
    }

    @Override
    public boolean tryAcquire(final long permits) {
      return this.semaphore.tryAcquire(Math.toIntExact(permits));
    }

    @Override
    public long drainPermits() {
      return this.semaphore.drainPermits();
    }

    @Override
    public long getAvailablePermits() {
      return this.semaphore.availablePermits();
    }

    @Override
    public int getQueueLength() {
      return this.semaphore.getQueueLength();
    }
  }

  public static void main(final String[] args) throws RunnerException, IOException {
    Main.main(args);
  }
}
