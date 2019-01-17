/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import org.apache.commons.lang3.ArrayUtils;
import org.cleversafe.util.ConcurrencyDetector;
import org.cleversafe.util.async.AsyncReadWriteLock.ReadLockToken;
import org.cleversafe.util.async.AsyncReadWriteLock.WriteLockToken;
import org.openjdk.jcstress.Main;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressMeta;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Mode;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.Signal;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIIIIII_Result;

final class AsyncReadWriteLockStressTests {
  private AsyncReadWriteLockStressTests() {
  }

  private interface AsyncReadWriteLockStressTest {
    AsyncReadWriteLock getLock();
  }

  /**
   * Some tests may require acquisition ordering (fairness). This interface is used as a marker for such tests,
   * restricting their use to implementations that declared themselves ordered
   */
  private interface OrderedAsyncReadWriteLockStressTest extends AsyncReadWriteLockStressTest {
  }

  /**
   * Test that {@link AsyncReadWriteLock#acquireReadLock() readers} can act together, but
   * {@link AsyncReadWriteLock#acquireWriteLock() writers} are exclusive of readers and writers.
   */
  // outcome matcher expects 4 readers and 4 writers
  @Outcome(id = "([01], ){4}0, 0, 0, 0", expect = Expect.ACCEPTABLE, desc = "only readers encountered concurrency")
  private static abstract class ReaderWriterExclusion implements AsyncReadWriteLockStressTest {
    private final AsyncReadWriteLock lock = getLock();
    /**
     * readers and writers will contend on the same concurrency check. It's acceptable for readers to be concurrent with
     * each other, but not with writers; if a reader and a writer were to overlap, the Outcome assertion of the writer
     * should fail the test because concurrency detectors will tend to be symmetric over enough runs.
     */
    private final ConcurrencyDetector detector = ConcurrencyDetector.newDetector();

    /** @return 1 if concurrency detected (acceptable among readers), 0 otherwise */
    int acquireRead() {
      return TestBlocking.getResult(this.lock.acquireReadLock()
          .map(lockToken -> {
            final boolean conflict = this.detector.test();
            lockToken.releaseReadLock();
            return conflict;
          })) ? 1 : 0;
    }

    /** @return 1 if concurrency detected, 0 otherwise */
    int acquireWrite() {
      return TestBlocking.getResult(this.lock.acquireWriteLock()
          .map(lockToken -> {
            final boolean conflict = this.detector.test();
            lockToken.releaseWriteLock();
            return conflict;
          })) ? 1 : 0;
    }
  }

  /**
   * Test that {@link AsyncReadWriteLock#tryReadLock() readers} can act together, but
   * {@link AsyncReadWriteLock#tryWriteLock() writers} are exclusive of readers and writers.
   */
  // outcome matcher expects 4 readers and 4 writers
  @Outcome(id = "1, 1, 1, 1, 0, 0, 0, 0", expect = Expect.ACCEPTABLE, desc = "all tryReads succeeded when unlocked")
  @Outcome(id = "0, 0, 0, 0, [^1]*1[^1]*", expect = Expect.ACCEPTABLE, desc = "one tryWrite succeeded when unlocked")
  private static abstract class TryReaderTryWriterExclusion implements AsyncReadWriteLockStressTest {
    private final AsyncReadWriteLock lock = getLock();

    /** @return 1 if acquired read lock */
    int tryRead() {
      return this.lock.tryReadLock().isPresent() ? 1 : 0;
    }

    /** @return 1 if acquired write lock */
    int tryWrite() {
      return this.lock.tryWriteLock().isPresent() ? 1 : 0;
    }
  }

  /**
   * mixes both acquire and try acquire of readers and writers
   */
  // outcome matcher expects 4 readers and 4 writers, 2/2 acquire and tryAcquire mix is reasonable
  @Outcome(id = "([01], ){4}0, 0, 0, 0", expect = Expect.ACCEPTABLE, desc = "only readers encountered concurrency")
  private static abstract class AcquireAndTryExclusion implements AsyncReadWriteLockStressTest {
    private final AsyncReadWriteLock lock = getLock();
    private final ConcurrencyDetector detector = ConcurrencyDetector.newDetector();

    int acquireRead() {
      return TestBlocking.getResult(this.lock.acquireReadLock()
          .map(lockToken -> {
            final boolean conflict = this.detector.test();
            lockToken.releaseReadLock();
            return conflict;
          })) ? 1 : 0;
    }

    int tryRead() {
      ReadLockToken lockToken;
      // loop in order to acquire eventually
      while ((lockToken = this.lock.tryReadLock().orElse(null)) == null) {
      }

      final boolean conflict = this.detector.test();
      lockToken.releaseReadLock();
      return conflict ? 1 : 0;
    }

    int acquireWrite() {
      return TestBlocking.getResult(this.lock.acquireWriteLock()
          .map(lockToken -> {
            final boolean conflict = this.detector.test();
            lockToken.releaseWriteLock();
            return conflict;
          })) ? 1 : 0;
    }

    int tryWrite() {
      WriteLockToken lockToken;
      // loop in order to acquire eventually
      while ((lockToken = this.lock.tryWriteLock().orElse(null)) == null) {
      }

      final boolean conflict = this.detector.test();
      lockToken.releaseWriteLock();
      return conflict ? 1 : 0;
    }
  }

  /**
   * Test that calls to {@link AsyncReadWriteLock#tryWriteLock()} won't succeed after a read lock is acquired
   */
  @Outcome(id = "TERMINATED", expect = Expect.ACCEPTABLE, desc = "tryWriteLock stopped succeeding after read acquired")
  private static abstract class TryWriteTerminatesOnReader implements AsyncReadWriteLockStressTest {
    private final AsyncReadWriteLock lock = getLock();

    void tryWriteLock() {
      WriteLockToken lockToken;
      while ((lockToken = this.lock.tryWriteLock().orElse(null)) != null) {
        lockToken.releaseWriteLock();
      }
    }

    void acquireReadLock() {
      TestBlocking.getResult(this.lock.acquireReadLock());
    }
  }

  /**
   * Test that calls to {@link AsyncReadWriteLock#tryWriteLock()} won't succeed after a write lock is acquired
   */
  @Outcome(id = "TERMINATED", expect = Expect.ACCEPTABLE, desc = "tryWriteLock stopped succeeding after write acquired")
  private static abstract class TryWriteTerminatesOnWriter implements AsyncReadWriteLockStressTest {
    private final AsyncReadWriteLock lock = getLock();

    void tryWriteLock() {
      WriteLockToken lockToken;
      while ((lockToken = this.lock.tryWriteLock().orElse(null)) != null) {
        lockToken.releaseWriteLock();
      }
    }

    void acquireWriteLock() {
      TestBlocking.getResult(this.lock.acquireWriteLock());
    }
  }

  /**
   * Test that calls to {@link AsyncReadWriteLock#tryReadLock()} won't succeed after a write lock is acquired
   */
  @Outcome(id = "TERMINATED", expect = Expect.ACCEPTABLE, desc = "tryWriteLock stopped succeeding after write acquired")
  private static abstract class TryReadTerminatesOnWriter implements AsyncReadWriteLockStressTest {
    private final AsyncReadWriteLock lock = getLock();

    void tryReadLock() {
      ReadLockToken lockToken;
      while ((lockToken = this.lock.tryReadLock().orElse(null)) != null) {
        lockToken.releaseReadLock();
      }
    }

    void acquireWriteLock() {
      TestBlocking.getResult(this.lock.acquireWriteLock());
    }
  }

  interface FairReadWriteLockStressTest extends OrderedAsyncReadWriteLockStressTest {
    @Override
    default AsyncReadWriteLock getLock() {
      return new FairAsyncReadWriteLock();
    }

    @JCStressTest
    @JCStressMeta(ReaderWriterExclusion.class)
    @State
    public static class FairReaderWriterExclusion extends ReaderWriterExclusion
        implements FairReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = acquireRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = acquireRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = acquireWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = acquireWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(TryReaderTryWriterExclusion.class)
    @State
    public static class FairTryReaderTryWriterExclusion extends TryReaderTryWriterExclusion
        implements FairReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = tryRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = tryRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = tryWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = tryWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(AcquireAndTryExclusion.class)
    @State
    public static class FairAcquireAndTryExclusion extends AcquireAndTryExclusion
        implements FairReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnReader.class)
    @State
    public static class FairTryWriteLockTerminatesOnReader extends TryWriteTerminatesOnReader
        implements FairReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireReadLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnWriter.class)
    @State
    public static class FairTryWriteLockTerminatesOnWriter extends TryWriteTerminatesOnWriter
        implements FairReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryReadTerminatesOnWriter.class)
    @State
    public static class FairTryReadLockTerminatesOnWriter extends TryReadTerminatesOnWriter
        implements FairReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryReadLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }
  }

  interface SemRWLockStressTest extends OrderedAsyncReadWriteLockStressTest {
    @Override
    default AsyncReadWriteLock getLock() {
      return new SemaphoreAsAsyncReadWriteLock(FairAsyncSemaphore::new, FairAsyncSemaphore.MAX_PERMITS);
    }

    @JCStressTest
    @JCStressMeta(ReaderWriterExclusion.class)
    @State
    public static class SemRWReaderWriterExclusion extends ReaderWriterExclusion
        implements SemRWLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = acquireRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = acquireRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = acquireWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = acquireWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(TryReaderTryWriterExclusion.class)
    @State
    public static class SemRWTryReaderTryWriterExclusion extends TryReaderTryWriterExclusion
        implements SemRWLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = tryRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = tryRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = tryWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = tryWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(AcquireAndTryExclusion.class)
    @State
    public static class SemRWAcquireAndTryExclusion extends AcquireAndTryExclusion
        implements SemRWLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnReader.class)
    @State
    public static class SemRWTryWriteLockTerminatesOnReader extends TryWriteTerminatesOnReader
        implements SemRWLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireReadLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnWriter.class)
    @State
    public static class SemRWTryWriteLockTerminatesOnWriter extends TryWriteTerminatesOnWriter
        implements SemRWLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryReadTerminatesOnWriter.class)
    @State
    public static class SemRWTryReadLockTerminatesOnWriter extends TryReadTerminatesOnWriter
        implements SemRWLockStressTest {
      @Actor
      public void actor() {
        tryReadLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }
  }

  interface FANRWLockStressTest extends OrderedAsyncReadWriteLockStressTest {
    @Override
    default AsyncReadWriteLock getLock() {
      return new AsyncNamedRWLockAsRWLock(new FairAsyncNamedReadWriteLock<>());
    }

    @JCStressTest
    @JCStressMeta(ReaderWriterExclusion.class)
    @State
    public static class FANRWReaderWriterExclusion extends ReaderWriterExclusion
        implements FANRWLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = acquireRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = acquireRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = acquireWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = acquireWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(TryReaderTryWriterExclusion.class)
    @State
    public static class FANRWTryReaderTryWriterExclusion extends TryReaderTryWriterExclusion
        implements FANRWLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = tryRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = tryRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = tryWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = tryWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(AcquireAndTryExclusion.class)
    @State
    public static class FANRWAcquireAndTryExclusion extends AcquireAndTryExclusion
        implements FANRWLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnReader.class)
    @State
    public static class FANRWTryWriteLockTerminatesOnReader extends TryWriteTerminatesOnReader
        implements FANRWLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireReadLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnWriter.class)
    @State
    public static class FANRWTryWriteLockTerminatesOnWriter extends TryWriteTerminatesOnWriter
        implements FANRWLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryReadTerminatesOnWriter.class)
    @State
    public static class FANRWTryReadLockTerminatesOnWriter extends TryReadTerminatesOnWriter
        implements FANRWLockStressTest {
      @Actor
      public void actor() {
        tryReadLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }
  }

  interface ReaderPriorityReadWriteLockStressTest extends AsyncReadWriteLockStressTest {
    @Override
    default AsyncReadWriteLock getLock() {
      return new ReaderPriorityAsyncReadWriteLock();
    }

    @JCStressTest
    @JCStressMeta(ReaderWriterExclusion.class)
    @State
    public static class ReaderPriorityReaderWriterExclusion extends ReaderWriterExclusion
        implements ReaderPriorityReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = acquireRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = acquireRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = acquireWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = acquireWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(TryReaderTryWriterExclusion.class)
    @State
    public static class ReaderPriorityTryReaderTryWriterExclusion extends TryReaderTryWriterExclusion
        implements ReaderPriorityReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = tryRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = tryRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = tryWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = tryWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(AcquireAndTryExclusion.class)
    @State
    public static class ReaderPriorityAcquireAndTryExclusion extends AcquireAndTryExclusion
        implements ReaderPriorityReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnReader.class)
    @State
    public static class ReaderPriorityTryWriteLockTerminatesOnReader extends TryWriteTerminatesOnReader
        implements ReaderPriorityReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireReadLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnWriter.class)
    @State
    public static class ReaderPriorityTryWriteLockTerminatesOnWriter extends TryWriteTerminatesOnWriter
        implements ReaderPriorityReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryReadTerminatesOnWriter.class)
    @State
    public static class ReaderPriorityTryReadLockTerminatesOnWriter extends TryReadTerminatesOnWriter
        implements ReaderPriorityReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryReadLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }
  }

  interface RPANRWLockReadWriteLockStressTest extends AsyncReadWriteLockStressTest {
    @Override
    default AsyncReadWriteLock getLock() {
      return new AsyncNamedRWLockAsRWLock(new ReaderPriorityAsyncNamedReadWriteLock<>());
    }

    @JCStressTest
    @JCStressMeta(ReaderWriterExclusion.class)
    @State
    public static class RPANRWLockReaderWriterExclusion extends ReaderWriterExclusion
        implements RPANRWLockReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = acquireRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = acquireRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = acquireWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = acquireWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(TryReaderTryWriterExclusion.class)
    @State
    public static class RPANRWLockTryReaderTryWriterExclusion extends TryReaderTryWriterExclusion
        implements RPANRWLockReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = tryRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = tryRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = tryWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = tryWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest
    @JCStressMeta(AcquireAndTryExclusion.class)
    @State
    public static class RPANRWLockAcquireAndTryExclusion extends AcquireAndTryExclusion
        implements RPANRWLockReadWriteLockStressTest {
      @Actor
      public void reader1(final IIIIIIII_Result r) {
        r.r1 = acquireRead();
      }

      @Actor
      public void reader2(final IIIIIIII_Result r) {
        r.r2 = acquireRead();
      }

      @Actor
      public void reader3(final IIIIIIII_Result r) {
        r.r3 = tryRead();
      }

      @Actor
      public void reader4(final IIIIIIII_Result r) {
        r.r4 = tryRead();
      }

      @Actor
      public void writer1(final IIIIIIII_Result r) {
        r.r5 = acquireWrite();
      }

      @Actor
      public void writer2(final IIIIIIII_Result r) {
        r.r6 = acquireWrite();
      }

      @Actor
      public void writer3(final IIIIIIII_Result r) {
        r.r7 = tryWrite();
      }

      @Actor
      public void writer4(final IIIIIIII_Result r) {
        r.r8 = tryWrite();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnReader.class)
    @State
    public static class RPANRWLockTryWriteLockTerminatesOnReader extends TryWriteTerminatesOnReader
        implements RPANRWLockReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireReadLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryWriteTerminatesOnWriter.class)
    @State
    public static class RPANRWLockTryWriteLockTerminatesOnWriter extends TryWriteTerminatesOnWriter
        implements RPANRWLockReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryWriteLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }

    @JCStressTest(Mode.Termination)
    @JCStressMeta(TryReadTerminatesOnWriter.class)
    @State
    public static class RPANRWLockTryReadLockTerminatesOnWriter extends TryReadTerminatesOnWriter
        implements RPANRWLockReadWriteLockStressTest {
      @Actor
      public void actor() {
        tryReadLock();
      }

      @Signal
      public void signal() {
        acquireWriteLock();
      }
    }
  }

  public static void main(final String[] args) throws Exception {
    Main.main(ArrayUtils.addAll(args, "-t", AsyncReadWriteLockStressTests.class.getName()));
  }
}
