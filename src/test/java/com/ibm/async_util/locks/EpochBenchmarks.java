/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import com.ibm.async_util.locks.ObservableEpoch.EpochToken;
import com.ibm.async_util.util.StageSupport;

public final class EpochBenchmarks {
  private EpochBenchmarks() {}

  @Fork(1)
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class Enter {
    ObservableEpoch epoch;

    @Param({"baseline", "impl", "rwlock"})
    String impl;

    @Setup
    public void setupBenchmark(final Blackhole bh) {
      this.epoch = Enter.getImpl(this.impl);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public Object enter() {
      final EpochToken t = this.epoch.enter().get();
      // do some work
      Blackhole.consumeCPU(20);
      // exit
      t.close();
      return t;
    }

    private static ObservableEpoch getImpl(final String impl) {
      switch (impl) {
        case "rwlock":
          return new MockRWLockEpoch();
        default:
          return EpochBenchmarks.getImpl(impl);
      }
    }

    private static class MockRWLockEpoch implements ObservableEpoch, EpochToken {
      private final Lock readLock = new ReentrantReadWriteLock().readLock();

      @Override
      public Optional<EpochToken> enter() {
        return this.readLock.tryLock() ? Optional.of(this) : Optional.empty();
      }

      @Override
      public CompletionStage<Boolean> terminate() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isTerminated() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
        this.readLock.unlock();
      }

      @Override
      public CompletionStage<Void> awaitCompletion() {
        throw new UnsupportedOperationException();
      }
    }
  }

  private static ObservableEpoch getImpl(final String impl) {
    switch (impl) {
      case "baseline":
        return new NoOpEpoch();
      case "impl":
        return new ObservableEpochImpl();
      default:
        throw new IllegalArgumentException("unknown epoch implementation:" + impl);
    }
  }

  private static class NoOpEpoch implements ObservableEpoch, EpochToken {
    private static final CompletionStage<Boolean> terminate =
        StageSupport.completedStage(false);
    private static final CompletionStage<Void> completion = StageSupport.completedStage(null);

    @Override
    public Optional<EpochToken> enter() {
      return Optional.of(this);
    }

    @Override
    public CompletionStage<Boolean> terminate() {
      return terminate;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public void close() {}

    @Override
    public CompletionStage<Void> awaitCompletion() {
      return completion;
    }
  }

  public static void main(final String args[]) throws RunnerException, IOException {
    Main.main(args);
  }
}
