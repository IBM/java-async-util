//
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
// @author: python
//
// Date: Jun 08, 2017
// ---------------------

package com.ibm.async_util;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.ibm.async_util.BiasedEithers.LeftEither;
import com.ibm.async_util.BiasedEithers.RightEither;



// @formatter:off

/**
 * A container which may yield one of two possible, mutually exclusive types.
 */
public interface Either<LEFT, RIGHT> {

  /**
   * @return true if this is a Left, false otherwise
   */
  default boolean isLeft() {
    return fold(
        left -> true,
        right -> false);
  }

  /**
   * @return true if this is a Right, false otherwise
   */
  default boolean isRight() {
    return fold(
        left -> false,
        right -> true);
  }

  /**
   * Access the object encompassed in this {@link Either} by providing two functions that
   * individually operate on the object's possible types. Apply f1 if this is a left or f2 if this
   * is a right.
   */
  <RESULT> RESULT fold(
      final Function<? super LEFT, ? extends RESULT> f1,
      final Function<? super RIGHT, ? extends RESULT> f2);

  /**
   * Access the object encompassed in this {@link Either} by providing two functions that
   * individually operate on the object's possible types. Apply c1 if this is a left or c2 if this
   * is a right.
   */
  default void forEach(
      final Consumer<? super LEFT> c1,
      final Consumer<? super RIGHT> c2) {
    fold(
        left -> {
          c1.accept(left);
          return null;
        }, right -> {
          c2.accept(right);
          return null;
        });
  }

  /**
   * Map the object encompassed in this {@link Either} by providing two functions that individually
   * operate on the object's possible types. Use {@link #left()} and
   * {@link LeftEither#map(Function)} or {@link #right()} and {@link RightEither#map(Function)} if
   * you only need to map to one type.<br>
   * <br>
   * NOTE: if the target types are the same, use {@link Either#fold(Function, Function)} instead.
   */
  default <A, B> Either<A, B> map(
      final Function<? super LEFT, ? extends A> f1,
      final Function<? super RIGHT, ? extends B> f2) {
    return fold(
        left -> Either.left(f1.apply(left)),
        right -> Either.right(f2.apply(right)));
  }

  /**
   * Project Left, providing convenience methods that operate only on the left element, if present.
   * 
   * @see LeftEither
   */
  default LeftEither<LEFT, RIGHT> left() {
    return fold(
        LeftEither::left,
        LeftEither::right);
  }

  /**
   * Project Right, providing convenience methods that operate only on the right element, if
   * present.
   * 
   * @see RightEither
   */
  default RightEither<LEFT, RIGHT> right() {
    return fold(
        RightEither::left,
        RightEither::right);
  }

  /**
   * Create a left Either<br>
   * <br>
   * Example:<br>
   * 
   * <pre>
   * Either<Long, String> left = Either.left(0L);
   * Either<Long, String> right = Either.right("string");
   * </pre>
   * 
   * @param a the left element of the new Either
   * @return
   * 
   * @see Either#right(Object)
   */
  static <A, B> Either<A, B> left(final A a) {
    return new Either<A, B>() {

      @Override
      public <RESULT> RESULT fold(
          final Function<? super A, ? extends RESULT> f1,
          final Function<? super B, ? extends RESULT> f2) {
        return f1.apply(a);
      }

      @Override
      public String toString() {
        if (a == null) {
          return "Left (null type): null";
        } else {
          return String.format("Left (%s): %s", a.getClass(), a);
        }
      }
    };
  }

  /**
   * Create a right Either<br>
   * <br>
   * Example:<br>
   * 
   * <pre>
   * Either<Long, String> left = Either.left(0L);
   * Either<Long, String> right = Either.right("string");
   * </pre>
   * 
   * @param a the right element of the new Either
   * @return
   * 
   * @see Either#left(Object)
   */
  static <A, B> Either<A, B> right(final B b) {
    return new Either<A, B>() {

      @Override
      public <RESULT> RESULT fold(
          final Function<? super A, ? extends RESULT> f1,
          final Function<? super B, ? extends RESULT> f2) {
        return f2.apply(b);
      }

      @Override
      public String toString() {
        if (b == null) {
          return "Right (null type): null";
        } else {
          return String.format("Right (%s): %s", b.getClass(), b);
        }
      }
    };
  }

  /**
   * Stream all present Left values from a collection of {@link Either} objects. Right values will
   * not be in the resulting stream.
   */
  static <LEFT, RIGHT> Stream<LEFT> leftStream(
      final Collection<? extends Either<LEFT, RIGHT>> elements) {
    return elements
        .stream()
        .filter(Either::isLeft)
        .map(element -> element.left().get());
  }

  /**
   * Stream all present Right values from a collection of {@link Either} objects. Left values will
   * not be in the resulting stream.
   */
  static <LEFT, RIGHT> Stream<RIGHT> rightStream(
      final Collection<? extends Either<LEFT, RIGHT>> elements) {
    return elements
        .stream()
        .filter(Either::isRight)
        .map(element -> element.right().get());
  }
}
// @formatter:on
