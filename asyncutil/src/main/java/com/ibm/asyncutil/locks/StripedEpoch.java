/* 
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.locks;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import com.ibm.asyncutil.locks.AsyncEpoch.EpochToken;
import com.ibm.asyncutil.util.StageSupport;

/**
 * An implementation of an {@link AsyncEpoch} that is scalable when many threads concurrently enter
 * the epoch, and has a small memory footprint when no contention is present. The approach is
 * similar to that of {@link LongAdder}.
 * <ul>
 * <li>Initially entrants/departers will update via a CAS of a single long which lives on the
 * parent.
 * <li>When two threads both try to update at the same time and one fails due to CAS contention, we
 * will allocate an array of 2 counters and map each thread to one of the two.
 * <li>Every subsequent collision the array will be doubled until it is larger than the number of
 * cores.
 * <li>Collisions after that will rehash the index associated with a thread. If threads are affine
 * to cores, on an oversubscribed system this will eventually lead to all threads on a given core to
 * mapping to the same index.
 * </ul>
 * 
 * This class uses counters that work similarly to a single field epoch implementation, except
 * rather than looping when encountering contention on {@link #enter()} or {@link #close()} it
 * instead reports the contention back. In the contended case, an array of counters are used which
 * are padded to avoid false sharing when threads are updating adjacent cells.
 * 
 * Resizes of the array happen by doubling the size of the array, copying over existing cells, and
 * CASing it back to cells. Termination cas's cells to a sentinel value so no subsequent resizes may
 * occur.
 * 
 * Threads are mapped to indices using a ThreadLocal via a cooperative contention mechanism. This is
 * similar to the one used by the j.u.c. via the probe field on Thread. See
 * {@link ThreadLocalRandom} for more details.
 * 
 * @author Ravi Khadiwala
 */
final class StripedEpoch implements AsyncEpoch, EpochToken {
  /**
   * The lsb reflects whether the cell has been terminated. The upper 63 bits are used for the
   * counter. Increment/decrement happens in units of 2s and always leave the lsb untouched. The
   * count can be extracted by dividing by 2. This approach is preferable to using the lower 63 bits
   * because we don't need to pay for a volatile write to initialize the counter fields because they
   * start at 0 instead of at the midpoint of 2^-63, 2^63.
   */
  private static final long TERMINATED = 1L;
  private static final int NCPU = Runtime.getRuntime().availableProcessors();
  /**
   * wipes out cells, means epoch has been terminated
   */
  private static final EpochCell[] DEAD_CELLS = new EpochCell[] {};

  private static final AtomicLongFieldUpdater<StripedEpoch> baseUpdater =
      AtomicLongFieldUpdater.newUpdater(StripedEpoch.class, "baseEntrants");
  private static final AtomicReferenceFieldUpdater<StripedEpoch, EpochCell[]> cellArrayUpdater =
      AtomicReferenceFieldUpdater.newUpdater(StripedEpoch.class, EpochCell[].class, "cells");

  /**
   * entrants go here when this epoch is uncontended
   */
  private volatile long baseEntrants;

  /**
   * If there is contention, cells will be initialized, and later grown via
   * {@link #growCells(EpochCell[]) on subsequent contention. If there is never contention, updates
   * will go to base. length is always power of 2, max length=2^(ceil(lg(NCPU)))
   **/
  private volatile EpochCell[] cells;
  private final CountdownFuture terminateFuture = new CountdownFuture();

  /**
   * If cells is null, try to enter base. if not null, skip right to entering a cell via
   * {@link #enterContended(EpochCell[]) If we entered base, or base was terminated - done.
   * otherwise initialize cells and try to enter a cell.
   */
  @Override
  public Optional<EpochToken> enter() {
    EpochCell[] localCells = this.cells;
    if (localCells == null) {
      final UpdateResult er = StripedEpoch.tryUpdate(baseUpdater, this, 2);
      if (er == UpdateResult.SUCCESS) {
        return Optional.of(this);
      } else if (er == UpdateResult.CLOSED) {
        return Optional.empty();
      } else {
        // cells was null, and we conflicted on base. Try to initialize it
        localCells = growCells(null);
      }
    }
    assert localCells != null;
    return enterContended(localCells);
  }

  /**
   * Called after we know we should take the contended path. After any contention on base occurs,
   * this path will always be taken.
   * 
   * Finds the index associated with this thread, and attempts to enter the corresponding cell. If
   * contention is encountered and we can still grow cells (have less than #cpus), we attempt to
   * grow and retry entering. If we already have the maximum number of cells, we rehash the thread's
   * index and retry.
   * 
   * @param localCells the value of {@code cells} we read, must not be {@code null}
   * @return an {@link EpochToken} corresponding to the cell we entered or empty.
   */
  private Optional<EpochToken> enterContended(EpochCell[] localCells) {
    int threadIndex = StripedEpoch.getProbe();
    // localCells guaranteed non-null
    while (true) {
      if (localCells == DEAD_CELLS) {
        return Optional.empty();
      }
      final EpochCell myCell = localCells[threadIndex & (localCells.length - 1)];
      final UpdateResult er = StripedEpoch.tryUpdate(cellUpdater, myCell, 2);
      if (er == UpdateResult.SUCCESS) {
        return Optional.of(this);
      } else if (er == UpdateResult.CLOSED) {
        return Optional.empty();
      } else if (er == UpdateResult.CONFLICT) {
        if (localCells.length < NCPU) {
          /*
           * We collided with another thread, and we haven't fully grown are cell array. Double the
           * size of the array to make collisions less likely, and then try again
           */
          localCells = growCells(localCells);
        } else {
          // we are fully grown but still conflicted. rehash.
          threadIndex = StripedEpoch.advanceProbe(threadIndex);
        }
      }
    }
  }

  @Override
  public void close() {
    EpochCell[] localCells = this.cells;
    if (localCells == null) {
      final UpdateResult er = StripedEpoch.tryUpdate(baseUpdater, this, -2);
      if (er == UpdateResult.SUCCESS) {
        return;
      } else if (er == UpdateResult.CLOSED) {
        this.terminateFuture.countdown();
        return;
      } else {
        localCells = growCells(null);
      }
    }
    assert localCells != null;
    departContended(localCells);
  }

  /**
   * Same as {@link #enterContended(EpochCell[])}, except we have to
   * {@link CountdownFuture#countdown()} the {@code terminateFuture} if we find out the epoch has
   * been terminated
   * 
   */
  private void departContended(EpochCell[] localCells) {
    int threadIndex = StripedEpoch.getProbe();
    // localCells guaranteed non-null

    while (true) {
      if (localCells == DEAD_CELLS) {
        this.terminateFuture.countdown();
        return;
      }
      final EpochCell myCell = localCells[threadIndex & (localCells.length - 1)];
      final UpdateResult er = StripedEpoch.tryUpdate(cellUpdater, myCell, -2);
      if (er == UpdateResult.CONFLICT) {
        if (localCells.length < NCPU) {
          /*
           * We collided with another thread, and we haven't fully grown are cell array. Double the
           * size of the array to make collisions less likely, and then try again
           */
          localCells = growCells(localCells);
        } else {
          // we are fully grown but still conflicted. rehash.
          threadIndex = StripedEpoch.advanceProbe(threadIndex);
        }
        continue;
      }
      if (er == UpdateResult.CLOSED) {
        this.terminateFuture.countdown();
      }
      return;
    }
  }

  /**
   * Try to grow cells (after hitting a conflict).
   * 
   * @return the new value of cells, which may have been created by this caller or installed by some
   *         other caller who beat us. In any case the returned value should have more cells than
   *         old unless it is {@code DEAD_CELLS}
   */
  private EpochCell[] growCells(final EpochCell[] old) {
    final EpochCell[] newCells;
    if (old == null) {
      // initial conflict, create new cells array
      newCells = new EpochCell[] {new EpochCell(), new EpochCell()};
    } else {
      // double the size of cells
      newCells = new EpochCell[old.length << 1];
      System.arraycopy(old, 0, newCells, 0, old.length);
      for (int i = old.length; i < newCells.length; i++) {
        newCells[i] = new EpochCell();
      }
    }
    if (cellArrayUpdater.compareAndSet(this, old, newCells)) {
      return newCells;
    } else {
      // someone beat us - read a new value of cells
      return this.cells;
    }
  }

  @Override
  public CompletionStage<Boolean> terminate() {
    final EpochCell[] localCells = cellArrayUpdater.getAndSet(this, DEAD_CELLS);
    if (localCells == DEAD_CELLS) {
      // we were beat
      return this.terminateFuture.thenApply(ig -> false);
    }

    /*
     * Once here, we are the exclusive terminator. cells can never be modified again, so it's safe
     * to iterate. As soon as dead cells has been set, departers will start leaving on
     * terminateFuture. We must find out how many total departs to expect and tell terminateFuture.
     * 
     * This is susceptible to overflow if there are more than long enter/departs over the lifetime
     * of the epoch, even if the number of active entrants is small. Luckily, that would take 300
     * years even with an enter/depart every nanosecond.
     */
    long entrantCount = StripedEpoch.terminateCounter(baseUpdater, this);
    if (localCells != null) {
      for (int i = 0; i < localCells.length; i++) {
        entrantCount += StripedEpoch.terminateCounter(cellUpdater, localCells[i]);
      }
    }
    this.terminateFuture.initialize(entrantCount);
    return this.terminateFuture;
  }


  @Override
  public boolean isTerminated() {
    /*
     * only terminated if every cell is terminated. This might be a little strict, but it's nice to
     * have the property that after isTerminated returns true, subsequent enters are guaranteed to
     * fail
     */
    final long localBase = this.baseEntrants;
    if ((localBase & TERMINATED) == 0) {
      return false;
    }

    // since at this point base is terminated, it's safe to read cells since it will never be
    // resized

    final EpochCell[] localCells = this.cells;
    if (localCells != null) {
      for (final EpochCell cell : localCells) {
        final long entrants = cell.entrants;
        if ((entrants & TERMINATED) == 0) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public CompletionStage<Void> awaitCompletion() {
    return StageSupport.voided(this.terminateFuture);
  }


  /* --- generic methods that take updaters that work on base or on a cell's counter field --- */

  /**
   * The result of trying to enter/depart a counter
   */
  enum UpdateResult {
    // the epoch count was successfully updated
    SUCCESS,
    // another thread tried to update at the same time
    CONFLICT,
    // this epoch cell has been terminated
    CLOSED
  }

  /**
   * Try to increment/decrement the entrant count for base or a cell
   *
   * @return {@link UpdateResult} possibly indicating that the counter was updated, otherwise it
   *         experienced contention with another thread or the epoch was already closed
   */
  private static <T> UpdateResult tryUpdate(final AtomicLongFieldUpdater<T> updater, final T obj,
      final int delta) {
    assert (delta & TERMINATED) == 0 : "increment/decrement shouldn't set terminate bit";
    final long curr = updater.get(obj);
    if ((curr & TERMINATED) != 0) {
      return UpdateResult.CLOSED;
    }
    final long newVal = curr + delta;
    if ((newVal & TERMINATED) != 0) {
      // would likely take centuries to happen?
      throw new IllegalStateException("excessively used epoch");
    }
    if (updater.compareAndSet(obj, curr, curr + delta)) {
      return UpdateResult.SUCCESS;
    } else {
      return UpdateResult.CONFLICT;
    }
  }

  /**
   * Terminates the counter, forbidding new entrants from entering
   * 
   * @return the number of entrants in the counter (possibly negative)
   */
  private static <T> long terminateCounter(final AtomicLongFieldUpdater<T> updater, final T obj) {
    // set the terminate bit (adding x + 1 is the same as x | 1 for even numbers)
    final long oldVal = updater.getAndAdd(obj, 1);
    if ((oldVal & TERMINATED) != 0) {
      throw new IllegalStateException("counter already terminated (should be impossible)");
    }
    return StripedEpoch.decode(oldVal);
  }

  /*
   * Padding so each thread can update their cell without false sharing.
   * 
   * @Contended requires user intervention (XX:-RestrictContended) and special compilation flags on
   * JDK9 (and we're trying to support 8 + 9). Instead manually pad. We assume 64 byte cache lines,
   * but pad 128 bytes on each side as done in the hotspot implementation. This is to compensate for
   * adjacent sector prefetch which can extend the false sharing to two cache lines. Inheritance is
   * a reliable trick to ensure the jvm does not reorder our fields.
   * 
   */

  private static class LeftPad {
    @SuppressWarnings("unused")
    protected volatile long l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, l14, l15;
  }

  private static final AtomicLongFieldUpdater<Value> cellUpdater =
      AtomicLongFieldUpdater.newUpdater(Value.class, "entrants");

  private static class Value extends LeftPad {
    protected volatile long entrants;
  }

  private static class RightPad extends Value {
    @SuppressWarnings("unused")
    protected volatile long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15;
  }

  /**
   * A padded counter that can be entered/closed. If an enter comes in this cell, the close should
   * happen on the same cell.
   */
  private static class EpochCell extends RightPad {
  }

  /**
   * Extract the (possibly negative) entrant count from the upper 63 bits of a count field.
   * 
   * @param count read from {@link EpochCell#entrants} or {@link #baseEntrants}
   * @return the number of entrants represented by this count
   */
  private static long decode(final long count) {
    return count >> TERMINATED;
  }

  private static ThreadLocal<int[]> probe =
      ThreadLocal.withInitial(() -> {
        final int next = ThreadLocalRandom.current().nextInt();
        // avoid 0 to allow xor rehash
        return new int[] {next == 0 ? 1 : next};
      });

  /**
   * see {@link ThreadLocalRandom#getProbe}
   */
  private static int getProbe() {
    return probe.get()[0];
  }

  /**
   * see {@link ThreadLocalRandom#advanceProbe}
   */
  private static int advanceProbe(int h) {
    h ^= h << 13; // xorshift
    h ^= h >>> 17;
    h ^= h << 5;
    probe.get()[0] = h;
    return h;
  }



  /**
   * After a cell has been terminated, departs will instead go to this class. As part of termination
   * this class will be notified for the total number of departs to expect, though departers may
   * start {@link #countdown() counting down} before terminate completely finishes. When all
   * expected departs have occured, {@code this} future will be completed.
   */
  private static class CountdownFuture extends CompletableFuture<Boolean> {
    volatile long count;
    static final AtomicLongFieldUpdater<CountdownFuture> countUpdater =
        AtomicLongFieldUpdater.newUpdater(CountdownFuture.class, "count");

    /**
     * Initialize the future with a count. If enough decrements have already happened before
     * initialize is called, this will complete the future
     * 
     * @param n how many decrements to invoke before the future is complete
     */
    void initialize(final long n) {
      if (n < 0) {
        throw new IllegalStateException("entrants can't be negative");
      } else if (this.count > 0) {
        throw new IllegalStateException("CountdownFuture initialized twice");
      }
      final long updated = countUpdater.addAndGet(this, n);
      if (updated < 0) {
        throw new IllegalStateException("Excessively closed epoch");
      }
      if (updated == 0) {
        final boolean completed = complete(true);
        assert completed;
      }
    }

    void countdown() {
      final long curr = countUpdater.decrementAndGet(this);
      if (curr == 0) {
        final boolean completed = complete(true);
        assert completed;
      }
    }
  }
}


