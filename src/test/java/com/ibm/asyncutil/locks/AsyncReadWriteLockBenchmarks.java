/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import com.ibm.asyncutil.util.StageSupport;
import com.ibm.asyncutil.util.TestUtil;

public final class AsyncReadWriteLockBenchmarks {
  private AsyncReadWriteLockBenchmarks() {}

  @Fork(1)
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class NoCompete {
    AsyncReadWriteLock rwlock;

    @Param({"baseline", "impl", "asyncSem"})
    String impl;

    @Setup
    public void setupBenchmark(final Blackhole bh) {
      this.rwlock = AsyncReadWriteLockBenchmarks.getImpl(this.impl);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object allReaders() {
      // unfortunately the countdown latch used in blocking can skew multithreaded tests. There
      // isn't a good alternative from the external interfaces to measure async operations
      final AsyncReadWriteLock.ReadLockToken t = TestUtil.join(this.rwlock.acquireReadLock());
      // do some work
      Blackhole.consumeCPU(20);
      t.releaseLock();
      return t;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object allWriters() {
      final AsyncReadWriteLock.WriteLockToken t = TestUtil.join(this.rwlock.acquireWriteLock());
      // do some work
      Blackhole.consumeCPU(20);
      t.releaseLock();
      return t;
    }
  }
  @Fork(1)
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class Compete3to1ReadersWriters {
    AsyncReadWriteLock rwlock;

    @Param({"baseline", "impl", "asyncSem"})
    String impl;

    @Setup
    public void setupBenchmark(final Blackhole bh) {
      this.rwlock = AsyncReadWriteLockBenchmarks.getImpl(this.impl);
    }

    @Benchmark
    @Group("compete")
    @GroupThreads(3)
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object readers() {
      final AsyncReadWriteLock.ReadLockToken t = TestUtil.join(this.rwlock.acquireReadLock());
      Blackhole.consumeCPU(20);
      t.releaseLock();
      return t;
    }

    @Benchmark
    @Group("compete")
    @GroupThreads(1)
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object writers() {
      final AsyncReadWriteLock.WriteLockToken t = TestUtil.join(this.rwlock.acquireWriteLock());
      Blackhole.consumeCPU(20);
      t.releaseLock();
      return t;
    }
  }

  private static AsyncReadWriteLock getImpl(final String impl) {
    switch (impl) {
      case "baseline":
        return new NoOpARWL();
      case "impl":
        return new FairAsyncReadWriteLock();
      case "asyncSem":
        return new SemaphoreAsAsyncReadWriteLock(FairAsyncSemaphore::new,
            FairAsyncSemaphore.MAX_PERMITS);
      default:
        throw new IllegalArgumentException("unknown ARWL implementation:" + impl);
    }
  }


  private static final class NoOpARWL
      implements AsyncReadWriteLock {
    private static final ReadLockToken READ = new ReadLockToken() {
      @Override
      public void releaseLock() {}
    };
    private static final WriteLockToken WRITE = new WriteLockToken() {
      @Override
      public void releaseLock() {}

      @Override
      public ReadLockToken downgradeLock() {
        return READ;
      }
    };

    private static final CompletionStage<ReadLockToken> readFuture =
        StageSupport.completedStage(READ);
    private static final CompletionStage<WriteLockToken> writeFuture =
        StageSupport.completedStage(WRITE);

    @Override
    public CompletionStage<WriteLockToken> acquireWriteLock() {
      return writeFuture;
    }

    @Override
    public CompletionStage<ReadLockToken> acquireReadLock() {
      return readFuture;
    }

    @Override
    public Optional<ReadLockToken> tryReadLock() {
      return Optional.of(READ);
    }

    @Override
    public Optional<WriteLockToken> tryWriteLock() {
      return Optional.of(WRITE);
    }
  }

  public static void main(final String[] args) throws RunnerException, IOException {
    Main.main(args);
  }
}
