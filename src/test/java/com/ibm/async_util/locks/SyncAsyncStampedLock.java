/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.async_util.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.StampedLock;

/**
 * An {@link AsyncStampedLock} backed by a synchronous {@link StampedLock} used for testing purposes
 */
class SyncAsyncStampedLock implements AsyncStampedLock {
  private final StampedLock lock = new StampedLock();
  private final Executor executor;

  public SyncAsyncStampedLock() {
    this(r -> new Thread(r).start());
  }

  public SyncAsyncStampedLock(final Executor executor) {
    this.executor = executor;
  }

  @Override
  public CompletionStage<ReadLockToken> acquireReadLock() {
    final CompletableFuture<ReadLockToken> future = new CompletableFuture<>();
    this.executor.execute(() -> {
      final long stamp = this.lock.readLock();
      future.complete(() -> this.lock.unlockRead(stamp));
    });
    return future;
  }

  @Override
  public Optional<ReadLockToken> tryReadLock() {
    final long stamp = this.lock.tryReadLock();
    return stamp == 0 ? Optional.empty() : Optional.of(() -> this.lock.unlockRead(stamp));
  }

  @Override
  public CompletionStage<WriteLockToken> acquireWriteLock() {
    final CompletableFuture<WriteLockToken> future = new CompletableFuture<>();
    this.executor.execute(() -> {
      final long stamp = this.lock.writeLock();
      future.complete(new StampWriteToken(stamp));
    });
    return future;
  }

  @Override
  public Optional<WriteLockToken> tryWriteLock() {
    final long stamp = this.lock.tryWriteLock();
    return stamp == 0 ? Optional.empty() : Optional.of(new StampWriteToken(stamp));
  }

  @Override
  public Stamp tryOptimisticRead() {
    final long stamp = this.lock.tryOptimisticRead();
    return stamp == 0 ? null : () -> this.lock.validate(stamp);
  }

  private class StampWriteToken implements WriteLockToken {
    private final long stamp;

    public StampWriteToken(final long stamp) {
      this.stamp = stamp;
    }

    @Override
    public void releaseWriteLock() {
      SyncAsyncStampedLock.this.lock.unlockWrite(this.stamp);
    }

    @Override
    public ReadLockToken downgradeLock() {
      final long readStamp = SyncAsyncStampedLock.this.lock.tryConvertToReadLock(this.stamp);
      if (readStamp == 0) {
        throw new IllegalStateException("downgraded lock not in locked state");
      }
      return () -> SyncAsyncStampedLock.this.lock.unlockRead(readStamp);
    }

  }
}
