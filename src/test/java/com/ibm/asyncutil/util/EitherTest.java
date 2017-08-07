/*
* Copyright (c) IBM Corporation 2017. All Rights Reserved.
* Project name: java-async-util
* This project is licensed under the Apache License 2.0, see LICENSE.
*/

package com.ibm.asyncutil.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class EitherTest {

  @Test
  public void testEitherLeft() {
    final Either<Integer, String> l = Either.left(5);

    int v = l.fold(Integer::intValue, String::length);
    Assert.assertEquals(5, v);

    final Either<Long, String> w = l.map(Long::valueOf, a -> a);
    v = w.fold(Long::intValue, String::length);
    Assert.assertEquals(5, v);

    final Either<Integer, String> r = l.fold(
        a -> Either.right(Integer.valueOf(a + 1).toString()),
        b -> Either.left(b.length()));
    Assert.assertEquals("6", r.fold(a -> a.toString(), b -> b));

    final AtomicInteger i = new AtomicInteger();
    l.forEach(i::addAndGet, b -> i.addAndGet(b.length()));
    Assert.assertEquals(5, i.get());
  }

  @Test
  public void testEitherRight() {
    final Either<Integer, String> l = Either.right("String!");

    int v = l.fold(Integer::intValue, String::length);
    Assert.assertEquals(7, v);

    final Either<Long, String> w = l.map(Long::valueOf, a -> a);
    v = w.fold(Long::intValue, String::length);
    Assert.assertEquals(7, v);

    final Either<Integer, String> r = l.fold(
        a -> Either.right(Integer.valueOf(a + 1).toString()),
        b -> Either.left(b.length()));
    Assert.assertEquals("7", r.fold(a -> a.toString(), b -> b));

    final AtomicInteger i = new AtomicInteger();
    l.forEach(i::addAndGet, b -> i.addAndGet(b.length()));
    Assert.assertEquals(7, i.get());
  }

  @Test
  public void testRightLeftEither() {
    final Either<Integer, String> l = Either.right("String!");

    int v = l.fold(Integer::intValue, String::length);
    Assert.assertEquals(7, v);

    final Either<Long, String> w = l.map(Long::valueOf, a -> a);
    v = w.fold(Long::intValue, String::length);
    Assert.assertEquals(7, v);

    final Either<Integer, String> r = l.fold(
        a -> Either.right(Integer.valueOf(a + 1).toString()),
        b -> Either.left(b.length()));
    Assert.assertEquals("7", r.fold(a -> a.toString(), b -> b));

    final AtomicInteger i = new AtomicInteger();
    l.forEach(i::addAndGet, b -> i.addAndGet(b.length()));
    Assert.assertEquals(7, i.get());
  }
}


