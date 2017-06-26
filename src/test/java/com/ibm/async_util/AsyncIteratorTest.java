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
// @author: rkhadiwala
//
// Date: Jun 18, 2015
// ---------------------

package com.ibm.async_util;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.async_util.AsyncIterator.End;


public class AsyncIteratorTest {

  @Test
  public void testConcat() throws Exception {
    final List<Integer> sizes = Arrays.asList(5, 3, 6);
    verifySorted(AsyncIterator.concat(sortedIts(sizes)), 14);
    verifySorted(AsyncIterator.concat(AsyncIterator.fromIterator(sortedIts(sizes).iterator())), 14);

  }

  private List<AsyncIterator<Integer>> sortedIts(final List<Integer> sizes) {
    final List<AsyncIterator<Integer>> iterators = new ArrayList<>();
    int count = 0;
    for (final int size : sizes) {
      final List<Integer> ints = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        ints.add(count);
        count++;
      }
      iterators.add(AsyncIterator.fromIterator(ints.iterator()));
    }
    return iterators;
  }

  // assert the iterator is the ascending range from 0 to expected size
  private void verifySorted(final AsyncIterator<Integer> concat, final int expectedSize)
      throws Exception {
    Either<Integer, End> curr = TestUtil.join(concat.nextFuture());
    int i = 0;
    while (curr.isLeft()) {
      Assert.assertEquals(i, curr.left().get().intValue());
      i++;
      curr = TestUtil.join(concat.nextFuture());
    }
    Assert.assertEquals(expectedSize, i);
  }

  private long size(final AsyncIterator<Integer> it) throws Exception {
    return TestUtil.join(it.collect(Collectors.counting()));
  }

  @Test
  public void testConcatStopsAfterIteratorIsEmpty() throws Exception {

    // validation for a bug where concat would call one more time after the previous call
    // to nextFuture returned empty

    // iterator throws assertion error if nextFuture is called after iterator empties
    final AtomicInteger count = new AtomicInteger();
    final AsyncIterator<Integer> touchyIterator = () -> {
      final int curr = count.getAndIncrement();
      if (curr == 0) {
        return CompletableFuture.completedFuture(Either.left(0));
      } else if (curr == 1) {
        return AsyncIterators.endFuture();
      }
      Assert.fail("called nextFuture() after previous call returned empty");
      return null;
    };

    CompletionStage<List<Integer>> collect = AsyncIterator
        .concat(AsyncIterator
            .fromIterator(Arrays.asList(touchyIterator, AsyncIterator.once(1)).iterator()))
        .collect(Collectors.toList());
    Assert.assertEquals(2, TestUtil.join(collect).size());
  }

  @Test
  public void testConcatNoIterators() throws Exception {
    final AsyncIterator<Integer> concat =
        AsyncIterator.concat(Collections.<AsyncIterator<Integer>>emptyList());
    Assert.assertEquals(0, size(concat));
    Assert.assertEquals(0, size(AsyncIterator.concat(AsyncIterator.empty())));
  }

  @Test
  public void testConcatWithAnEmptyIterator2() throws Exception {
    final List<AsyncIterator<Integer>> lis =
        Arrays.asList(AsyncIterator.empty(), AsyncIterator.empty());
    TestUtil.join(AsyncIterator.concat(lis).consume());
  }


  @Test
  public void testConcatWithAnEmptyIterator() throws Exception {
    // try an empty iterator in all 3 positions
    for (int i = 0; i < 3; i++) {
      final List<Integer> sizes = new ArrayList<>();
      for (int j = 0; j < 3; j++) {
        sizes.add(j == i ? 0 : 10);
      }
      verifySorted(AsyncIterator.concat(sortedIts(sizes)), 2 * 10);
      verifySorted(AsyncIterator.concat(AsyncIterator.fromIterator(sortedIts(sizes).iterator())),
          2 * 10);

    }

  }

  @Test
  public void testConcatException() throws Exception {
    final Supplier<List<AsyncIterator<Integer>>> it = () -> {
      final List<AsyncIterator<Integer>> iterators = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        if (i == 1) {
          iterators.add(AsyncIterator.error(new Exception("test")));
        } else {
          iterators.add(AsyncIterator.range(0, 10, 1));
        }
      }
      return iterators;
    };
    AsyncIterator<Integer> concat = AsyncIterator.concat(it.get());
    verifyExceptionThrown(concat);

    concat = AsyncIterator.concat(AsyncIterator.fromIterator(it.get().iterator()));
    verifyExceptionThrown(concat);
  }

  private void verifyExceptionThrown(final AsyncIterator<Integer> iterator) {
    Exception e = null;
    Either<Integer, End> curr;
    do {
      try {
        curr = TestUtil.join(iterator.nextFuture());
      } catch (final Exception e1) {
        e = e1;
        break;
      }

    } while (curr.isLeft());
    Assert.assertNotNull(e);
  }

  @Test
  public void testConcatStackOverflow() throws Exception {
    final List<AsyncIterator<Integer>> iterators = new ArrayList<AsyncIterator<Integer>>();
    for (int i = 0; i < 100000; i++) {
      iterators.add(AsyncIterator.empty());
    }
    TestUtil.join(AsyncIterator.concat(iterators).consume());

    final AsyncIterator<AsyncIterator<Integer>> iteratorIterator =
        AsyncIterator.fromIterator(iterators.iterator());

    TestUtil.join(AsyncIterator.concat(iteratorIterator).consume());
  }

  @Test
  public void testForEachEmptyList() {
    final AsyncIterator<Integer> iterator = AsyncIterator.empty();
    TestUtil.join(iterator.forEach(ignore -> Assert.fail()));
  }

  @Test
  public void testForEach() {
    final List<Integer> ints = new ArrayList<>();
    for (int i = 0; i < 100000; i++) {
      ints.add(i);
    }
    final List<Integer> results = new ArrayList<>();
    final AsyncIterator<Integer> iterator = AsyncIterator.fromIterator(ints.iterator());
    TestUtil.join(iterator.forEach(results::add));
    Assert.assertEquals(ints, results);
  }

  @Test
  public void testTakeWhile() {
    final List<Integer> results = new ArrayList<>();
    final AsyncIterator<Integer> iterator = intIterator(100000);
    iterator.takeWhile(i -> {
      Assert.assertTrue(i <= 100);
      return i < 100;
    }).forEach(t -> {
      Assert.assertTrue(t < 100);
      results.add(t);
    });
    Assert.assertEquals(100, results.size());
  }

  @Test
  public void testForEachException() {
    final AsyncIterator<Integer> iterator = AsyncIterator.error(new IOException("test"));
    try {
      TestUtil.join(iterator.consume());
      Assert.fail("Should have been an exception");
    } catch (final Exception e) {
      // expected
    }
  }

  @Test
  public void testMap() {
    final AsyncIterator<Integer> x = intIterator(1000);
    final AsyncIterator<Integer> mapped = x.map(c -> CompletableFuture.completedFuture(c + 1));
    final List<Integer> list = TestUtil.join(mapped.take(1001).collect(Collectors.toList()));
    Assert.assertEquals(1000, list.size());
    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(i + 1, list.get(i).intValue());
    }
  }

  @Test
  public void testConvert() {
    final AsyncIterator<Integer> x = intIterator(1000);
    final AsyncIterator<Integer> mapped = x.convert(c -> c + 1);
    final List<Integer> list = TestUtil.join(mapped.take(1001).collect(Collectors.toList()));
    Assert.assertEquals(1000, list.size());
    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(i + 1, list.get(i).intValue());
    }
  }

  @Test
  public void testMapAhead() {
    final AsyncIterator<Integer> x = intIterator(1000);
    final AsyncIterator<Integer> mapped =
        x.mapAhead(c -> CompletableFuture.completedFuture(c + 1), 2);
    final List<Integer> list = TestUtil.join(mapped.take(1001).collect(Collectors.toList()));
    Assert.assertEquals(1000, list.size());
    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(i + 1, list.get(i).intValue());
    }
  }

  // @Test
  // public void testMapAheadParallel()
  // throws TimeoutException, InterruptedException {
  // final ThreadPoolExecutor ex =
  // TestSafeExecutors.INSTANCE.newExecutor(10, "async-iterator-test", true);
  // final AsyncIterator<Integer> it = intIterator(10);
  //
  // final AsyncIterator<Integer> mapped = it.mapAhead(i -> {
  // final SimpleFuture<Integer, VoidException> f = new SimpleFuture<Integer, VoidException>();
  // ex.submit(() -> {
  // try {
  // Thread.sleep(1000);
  // } catch (final InterruptedException e) {
  // }
  // f.processSuccess(i);
  // });
  // return f;
  // }, 10);
  //
  // final List<Integer> lis =
  // TestUtil.join(mapped.collect(Collectors.toList()), 1900, TimeUnit.MILLISECONDS);
  // Assert.assertEquals(TestUtil.join(intIterator(10).collect(Collectors.toList())), lis);
  // ex.shutdown();
  // ex.awaitTermination(20, TimeUnit.MILLISECONDS);
  // }
  //
  // @Test
  // public void testMapAheadParallelLonger()
  // throws VoidException, TimeoutException, InterruptedException {
  // final ThreadPoolExecutor ex =
  // TestSafeExecutors.INSTANCE.newExecutor(10, "async-iterator-test", true);
  // final AsyncIterator<Integer> it = intIterator(100);
  //
  // final AsyncIterator<Integer> mapped = it.mapAhead(i -> {
  // final SimpleFuture<Integer, VoidException> f = new SimpleFuture<Integer, VoidException>();
  // ex.submit(() -> {
  // try {
  // Thread.sleep(100);
  // } catch (final InterruptedException e) {
  // }
  // f.processSuccess(i);
  // });
  // return f;
  // }, 10);
  //
  // final List<Integer> lis =
  // TestUtil.join(mapped.collect(Collectors.toList()), 1900, TimeUnit.MILLISECONDS);
  // Assert.assertEquals(TestUtil.join(intIterator(100).collect(Collectors.toList())), lis);
  // ex.shutdown();
  // ex.awaitTermination(20, TimeUnit.MILLISECONDS);
  // }
  //
  // @Test
  // public void testMapAheadParallelRandomSleeps()
  // throws VoidException, TimeoutException, InterruptedException {
  // final ThreadPoolExecutor ex =
  // TestSafeExecutors.INSTANCE.newExecutor(10, "async-iterator-test", true);
  // final AsyncIterator<Integer> it = intIterator(100);
  //
  // final AsyncIterator<Integer> mapped = it.mapAhead(i -> {
  // final SimpleFuture<Integer, VoidException> f = new SimpleFuture<Integer, VoidException>();
  // ex.submit(() -> {
  // try {
  // Thread.sleep((long) (Math.random() * 100));
  // } catch (final InterruptedException e) {
  // }
  // f.processSuccess(i);
  // });
  // return f;
  // }, 10);
  //
  // final List<Integer> lis =
  // TestUtil.join(mapped.collect(Collectors.toList()), 1900, TimeUnit.MILLISECONDS);
  // Assert.assertEquals(TestUtil.join(intIterator(100).collect(Collectors.toList())), lis);
  // ex.shutdown();
  // ex.awaitTermination(20, TimeUnit.MILLISECONDS);
  // }

  @Test
  public void testFlatMap() {
    // should take [0,1,2,...,999] -> [1,2,2,3,3,3,4,4,4,4,...999,999]
    final int count = 1000;
    final AsyncIterator<Integer> x = intIterator(count);
    final AsyncIterator<Integer> flatMapped = x.flatMap(c -> repeat(c, c));
    final List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < i; j++) {
        expected.add(i);
      }
    }
    final List<Integer> list =
        TestUtil.join(flatMapped.take(expected.size() + 1).collect(Collectors.toList()));
    Assert.assertEquals(expected, list);
  }

  @Test
  public void testFlatMapIsLazy() {
    final AtomicBoolean called = new AtomicBoolean();
    final AsyncIterator<Integer> x = intIterator(10);
    final AsyncIterator<Integer> y = x.flatMap(c -> {
      called.set(true);
      return repeat(c, c);
    });
    Assert.assertFalse(called.get());
    TestUtil.join(y.nextFuture());
    Assert.assertTrue(called.get());
  }

  @Test
  public void testMapIsLazy() {
    final AtomicBoolean called = new AtomicBoolean();
    final AsyncIterator<Integer> x = intIterator(10);
    final AsyncIterator<Object> y = x.map(i -> {
      called.set(true);
      return CompletableFuture.completedFuture(i);
    });
    Assert.assertFalse(called.get());
    TestUtil.join(y.nextFuture());
    Assert.assertTrue(called.get());
  }

  @Test
  public void testConvertIsLazy() {
    final AtomicBoolean called = new AtomicBoolean();
    final AsyncIterator<Integer> x = intIterator(10);
    final AsyncIterator<Object> y = x.convert(i -> {
      called.set(true);
      return i;
    });
    Assert.assertFalse(called.get());
    TestUtil.join(y.nextFuture());
    Assert.assertTrue(called.get());
  }

  @Test
  public void testFilter() {
    final int count = 100000;
    final Predicate<Integer> filterFun = i -> i % 2 == 0 && i > (count / 2);
    final CompletionStage<List<Integer>> actual =
        intIterator(count).filter(filterFun).take((count / 2) + 1).collect(Collectors.toList());
    final List<Integer> expected =
        IntStream.range(0, count).boxed().filter(filterFun).collect(Collectors.toList());
    Assert.assertEquals(expected, TestUtil.join(actual));
  }

  @Test
  public void testFold() {
    final int count = 100000;
    final AsyncIterator<Integer> it = intIterator(count);
    final int actual = TestUtil.join(it.fold((a, b) -> a + b, 0));
    final int expected = (count * (count - 1)) / 2;
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRange() throws Exception {
    List<Integer> range = TestUtil.join(AsyncIterator.range(5, 0, -1).collect(Collectors.toList()));
    Assert.assertEquals(Arrays.asList(5, 4, 3, 2, 1), range);

    range = TestUtil.join(AsyncIterator.range(0, 0, 1).collect(Collectors.toList()));
    Assert.assertEquals(Arrays.asList(), range);

    range = TestUtil.join(AsyncIterator.range(0, 5, 1).collect(Collectors.toList()));
    Assert.assertEquals(Arrays.asList(0, 1, 2, 3, 4), range);

    range = TestUtil.join(AsyncIterator.range(0, 5, 2).collect(Collectors.toList()));
    Assert.assertEquals(Arrays.asList(0, 2, 4), range);

    range = TestUtil.join(AsyncIterator.range(-3, 0, 1).collect(Collectors.toList()));
    Assert.assertEquals(Arrays.asList(-3, -2, -1), range);

    try {
      TestUtil.join(AsyncIterator.range(-3, 0, 0).collect(Collectors.toList()));
      Assert.fail("expected illegal arg exception");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testZip() throws Exception {
    final List<Integer> zipped = TestUtil.join(AsyncIterator
        .zipWith(intIterator(5), intIterator(100), (x, y) -> x + y).collect(Collectors.toList()));
    final List<Integer> expected =
        IntStream.range(0, 5).map(i -> i + i).boxed().collect(Collectors.toList());
    Assert.assertEquals(expected, zipped);
  }

  @Test
  public void testCollectors() throws Exception {

    // test for a non identity finisher
    final AsyncIterator<Integer> x = intIterator(10);
    Assert.assertEquals(TestUtil.join(x.collect(Collectors.averagingInt(i -> i))), 4.5, .001);

    // test for an identity finisher
    final AsyncIterator<Integer> y = intIterator(10);
    Assert.assertEquals(TestUtil.join(y.collect(Collectors.toList())),
        IntStream.range(0, 10).boxed().collect(Collectors.toList()));
  }

  @Test
  public void testCollect() throws Exception {

    final Supplier<List<Integer>> supp = () -> new ArrayList<Integer>();
    final BiConsumer<List<Integer>, Integer> acc = (l, i) -> {
      l.add(i);
      return;
    };
    final AsyncIterator<Integer> x = intIterator(10);
    Assert.assertEquals(TestUtil.join(x.collect(supp, acc)),
        IntStream.range(0, 10).boxed().collect(Collectors.toList()));
  }

  @Test
  public void testUnordered() throws Exception {
    final List<CompletableFuture<Integer>> futures = IntStream.range(0, 5)
        .mapToObj(ig -> new CompletableFuture<Integer>()).collect(Collectors.toList());

    final AsyncIterator<Integer> it = AsyncIterator.unordered(futures);
    // complete 0..5 in random order
    for (final int i : new int[] {1, 4, 2, 0, 3}) {
      final CompletionStage<Integer> nextFuture = it.nextFuture().thenApply(e -> e.left().get());
      Assert.assertFalse(nextFuture.toCompletableFuture().isDone());
      futures.get(i).complete(i);
      Assert.assertTrue(nextFuture.toCompletableFuture().isDone());
      Assert.assertEquals(i, TestUtil.join(nextFuture).intValue());
    }
    Assert.assertFalse(TestUtil.join(it.nextFuture()).isLeft());
  }

  @Test
  public void testBatch() {
    final List<String> list =
        Arrays.asList("a", "123", "foo", "bar", "b", "c", "q", "w", "e", "r", "t", "y");

    /*
     * this test batches the given iter of strings into lists that have a predefined size limit. The
     * batch predicate checks the batch's size against this limit, and stops adding to the batch
     * when the limit is reached
     */

    final int[] sizeLimits = new int[] {2, 3, 0, 1, 4, 2};

    final List<Collection<String>> expected = Arrays.asList(Arrays.asList("a", "123"),
        Arrays.asList("foo", "bar", "b"), Collections.emptyList(), Arrays.asList("c"),
        Arrays.asList("q", "w", "e", "r"), Arrays.asList("t", "y"));

    final Queue<Integer> sizeLimitsQueue =
        Arrays.stream(sizeLimits).boxed().collect(ArrayDeque::new, (q, val) -> {
          q.add(val);
        }, (q1, q2) -> {
          q1.addAll(q2);
        });

    @SuppressWarnings("serial")
    class LimitedList<T> extends ArrayList<T> {
      // this poll will fail if the iterator tries to create more batches than necessary.
      // (not sufficiently lazy)
      final int limit = sizeLimitsQueue.poll();
    }

    final AsyncIterator<Collection<String>> iter = AsyncIterator.fromIterator(list.iterator())
        .batch(Collectors.toCollection(() -> new LimitedList<>()), (acc, str) -> {
          @SuppressWarnings("unchecked")
          final LimitedList<String> l = ((LimitedList<String>) acc);
          return l.size() < l.limit;
        });

    Assert.assertEquals(expected, TestUtil.join(iter.collect(Collectors.toList())));
    Assert.assertFalse(TestUtil.join(iter.nextFuture()).isLeft());
  }

  @Test
  public void testBatchEmpty() throws Exception {
    Assert.assertFalse(
        TestUtil.join(AsyncIterator.empty().batch(Collectors.toList(), (a, b) -> true).nextFuture())
            .isLeft());
  }

  @Test
  public void testBatchEmptyLazyBatch() throws Exception {
    final Supplier<Collection<Object>> supplier = () -> {
      throw new AssertionError("batching wasn't lazy enough");
    };
    Assert.assertFalse(TestUtil.join(
        AsyncIterator.empty().batch(Collectors.toCollection(supplier), (a, b) -> true).nextFuture())
        .isLeft());
  }

  @Test
  public void testBatchCounting() throws Exception {
    final List<String> list =
        Arrays.asList("a", "123", "foo", "bar", "b", "c", "q", "w", "e", "r", "t");

    final List<Collection<String>> expected = Arrays.asList(Arrays.asList("a", "123", "foo", "bar"),
        Arrays.asList("b", "c", "q", "w"), Arrays.asList("e", "r", "t"));

    Assert.assertEquals(expected, TestUtil.join(AsyncIterator.fromIterator(list.iterator())
        .batch(Collectors.toList(), 4).collect(Collectors.toList())));

    Assert.assertEquals(Collections.singleton(new HashSet<>(list)),
        TestUtil.join(AsyncIterator.fromIterator(list.iterator())
            .batch(Collectors.toSet(), list.size()).collect(Collectors.toSet())));
  }


  AsyncIterator<Integer> intIterator(final int size) {
    return AsyncIterator.fromIterator(IntStream.range(0, size).iterator());
  }

  AsyncIterator<Integer> repeat(final int n, final int size) {
    return AsyncIterator.repeat(n).take(size);
  }

}
