/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package org.cleversafe.util.async;

import java.util.ConcurrentModificationException;
import java.util.Optional;

import org.cleversafe.util.async.AsyncReadWriteLock.ReadLockToken;
import org.cleversafe.util.async.AsyncReadWriteLock.WriteLockToken;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractAsyncNamedReadWriteLockTest extends AbstractAsyncReadWriteLockTest {

  protected abstract <T> AsyncNamedReadWriteLock<T> getNamedReadWriteLock();

  protected abstract boolean isEmpty(AsyncNamedReadWriteLock<?> narwls);

  @Override
  protected final AsyncReadWriteLock getReadWriteLock() {
    return new AsyncNamedRWLockAsRWLock(getNamedReadWriteLock());
  }

  @Test
  public final void testExclusivity() throws Exception {
    final AsyncNamedReadWriteLock<String> narwls = getNamedReadWriteLock();
    final String s1 = "abc";
    final String s2 = "123";

    Assert.assertTrue(isEmpty(narwls));
    final Future<ReadLockToken> read1_1 =
        narwls.acquireReadLock(s1);
    Assert.assertFalse(isEmpty(narwls));
    Assert.assertTrue(read1_1.isDone());

    final Future<ReadLockToken> read1_2 =
        narwls.acquireReadLock(s1);
    Assert.assertTrue(read1_2.isDone());

    final Future<WriteLockToken> write1_1 =
        narwls.acquireWriteLock(s1);
    Assert.assertFalse(write1_1.isDone());

    TestBlocking.getResult(read1_2).releaseReadLock();
    Assert.assertFalse(write1_1.isDone());

    // s2 can read or write while s1 is occupied
    Assert.assertTrue(narwls.acquireReadLock(s2)
        .mapFinally(readLock -> readLock.releaseReadLock()).isDone());
    Assert.assertTrue(narwls.acquireWriteLock(s2)
        .mapFinally(writeLock -> writeLock.releaseWriteLock()).isDone());
    Assert.assertTrue(narwls.acquireReadLock(s2)
        .mapFinally(readLock -> readLock.releaseReadLock()).isDone());
    Assert.assertFalse(isEmpty(narwls));

    TestBlocking.getResult(read1_1).releaseReadLock();
    Assert.assertTrue(write1_1.isDone());

    Assert.assertFalse(narwls.acquireReadLock(s1)
        .mapFinally(readLock -> readLock.releaseReadLock()).isDone());

    TestBlocking.getResult(write1_1).releaseWriteLock();
    Assert.assertTrue(isEmpty(narwls));
  }

  private static class MutableKey {
    int value = 0;

    @Override
    public int hashCode() {
      return this.value;
    }
  }

  @Test(expected = ConcurrentModificationException.class)
  public final void testReadNameModificationException() {
    final AsyncNamedReadWriteLock<MutableKey> lock = getNamedReadWriteLock();
    final MutableKey key = new MutableKey();
    final ReadLockToken readToken = TestBlocking.getResult(lock.acquireReadLock(key));
    key.value = 1;
    readToken.releaseReadLock();
  }

  @Test(expected = ConcurrentModificationException.class)
  public final void testWriteNameModificationException() {
    final AsyncNamedReadWriteLock<MutableKey> lock = getNamedReadWriteLock();
    final MutableKey key = new MutableKey();
    final WriteLockToken writeToken = TestBlocking.getResult(lock.acquireWriteLock(key));
    key.value = 1;
    writeToken.releaseWriteLock();
  }

  public static abstract class AbstractAsyncNamedReadWriteLockFairnessTest
      extends AbstractAsyncNamedReadWriteLockTest {
    @Test
    public void testFairness() throws Exception {
      final AsyncNamedReadWriteLock<String> narwls = getNamedReadWriteLock();
      final String s = "abc";

      final Future<WriteLockToken> write1 =
          narwls.acquireWriteLock(s);
      Assert.assertTrue(write1.isDone());

      // under write lock
      final Future<ReadLockToken> read1 =
          narwls.acquireReadLock(s);
      final Future<ReadLockToken> read2 =
          narwls.acquireReadLock(s);
      final Future<ReadLockToken> read3 =
          narwls.acquireReadLock(s);
      Assert.assertFalse(read1.isDone());
      Assert.assertFalse(read2.isDone());
      Assert.assertFalse(read3.isDone());

      final Future<WriteLockToken> write2 =
          narwls.acquireWriteLock(s);
      Assert.assertFalse(write2.isDone());

      final Future<ReadLockToken> read4 =
          narwls.acquireReadLock(s);
      final Future<ReadLockToken> read5 =
          narwls.acquireReadLock(s);
      final Future<ReadLockToken> read6 =
          narwls.acquireReadLock(s);
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());

      final Future<WriteLockToken> write3 =
          narwls.acquireWriteLock(s);
      Assert.assertFalse(write3.isDone());

      TestBlocking.getResult(write1).releaseWriteLock();
      Assert.assertTrue(read1.isDone());
      Assert.assertTrue(read2.isDone());
      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());

      TestBlocking.getResult(read1).releaseReadLock();
      TestBlocking.getResult(read2).releaseReadLock();
      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());

      // now under read lock (read3 still active)
      final Future<ReadLockToken> read7 =
          narwls.acquireReadLock(s);
      final Future<WriteLockToken> write4 =
          narwls.acquireWriteLock(s);

      Assert.assertTrue(read3.isDone());
      Assert.assertFalse(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(read3).releaseReadLock();
      Assert.assertTrue(write2.isDone());
      Assert.assertFalse(read4.isDone());
      Assert.assertFalse(read5.isDone());
      Assert.assertFalse(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(write2).releaseWriteLock();
      Assert.assertTrue(read4.isDone());
      Assert.assertTrue(read5.isDone());
      Assert.assertTrue(read6.isDone());
      Assert.assertFalse(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(Futures.all(read4, read5, read6))
          .forEach(readLock -> readLock.releaseReadLock());
      Assert.assertTrue(write3.isDone());
      Assert.assertFalse(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(write3).releaseWriteLock();
      Assert.assertTrue(read7.isDone());
      Assert.assertFalse(write4.isDone());

      TestBlocking.getResult(read7).releaseReadLock();
      Assert.assertTrue(write4.isDone());

      TestBlocking.getResult(write4).releaseWriteLock();
      Assert.assertTrue(isEmpty(narwls));
    }
  }

  public static abstract class AbstractAsyncNamedReadWriteLockReaderPriorityTest
      extends AbstractAsyncNamedReadWriteLockTest {
  }
}


class AsyncNamedRWLockAsRWLock implements AsyncReadWriteLock {
  private static final Object KEY = new Object();
  private final AsyncNamedReadWriteLock<Object> lock;

  public AsyncNamedRWLockAsRWLock(final AsyncNamedReadWriteLock<Object> lock) {
    this.lock = lock;
  }

  @Override
  public Future<ReadLockToken> acquireReadLock() {
    return this.lock.acquireReadLock(KEY);
  }

  @Override
  public Future<WriteLockToken> acquireWriteLock() {
    return this.lock.acquireWriteLock(KEY);
  }

  @Override
  public Optional<ReadLockToken> tryReadLock() {
    return this.lock.tryReadLock(KEY);
  }

  @Override
  public Optional<WriteLockToken> tryWriteLock() {
    return this.lock.tryWriteLock(KEY);
  }

}
