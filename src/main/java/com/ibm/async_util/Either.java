package com.ibm.async_util;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/** A container which may yield one of two possible, mutually exclusive types. */
public interface Either<L, R> {

  /** @return true if this is a Left, false otherwise */
  default boolean isLeft() {
    return fold(left -> true, right -> false);
  }

  /** @return true if this is a Right, false otherwise */
  default boolean isRight() {
    return fold(left -> false, right -> true);
  }

  /**
   * Access the object encompassed in this {@link Either} by providing two functions that
   * individually operate on the object's possible types. Apply f1 if this is a left or f2 if this
   * is a right.
   */
  <V> V fold(final Function<? super L, ? extends V> f1, final Function<? super R, ? extends V> f2);

  /**
   * Access the object encompassed in this {@link Either} by providing two functions that
   * individually operate on the object's possible types. Apply c1 if this is a left or c2 if this
   * is a right.
   */
  default void forEach(final Consumer<? super L> c1, final Consumer<? super R> c2) {
    fold(
        left -> {
          c1.accept(left);
          return null;
        },
        right -> {
          c2.accept(right);
          return null;
        });
  }

  /**
   * Map the object encompassed in this {@link Either} by providing two functions that individually
   * operate on the object's possible types.
   */
  default <A, B> Either<A, B> map(
      final Function<? super L, ? extends A> f1, final Function<? super R, ? extends B> f2) {
    return fold(left -> Either.left(f1.apply(left)), right -> Either.right(f2.apply(right)));
  }

  default <V> Either<L, V> map(final Function<? super R, ? extends V> f) {
    return fold(left -> Either.left(left), r -> Either.right(f.apply(r)));
  }

  default <V> Either<L, V> flatMap(final Function<? super R, ? extends Either<L, V>> f) {
    return fold(left -> Either.left(left), f);
  }

  default Optional<L> left() {
    return fold(Optional::of, r -> Optional.empty());
  }

  default Optional<R> right() {
    return fold(l -> Optional.empty(), Optional::of);
  }

  /**
   * Create a left Either<br>
   *
   * @param a the left element of the new Either
   * @return an Either with a left value
   * @see Either#right(Object)
   */
  static <A, B> Either<A, B> left(final A a) {
    return new Either<A, B>() {

      @Override
      public <V> V fold(
          final Function<? super A, ? extends V> f1,
          final Function<? super B, ? extends V> f2) {
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
   *
   * @param b the right element of the new Either
   * @return An Either with a right value
   * @see Either#left(Object)
   */
  static <A, B> Either<A, B> right(final B b) {
    return new Either<A, B>() {

      @Override
      public <V> V fold(
          final Function<? super A, ? extends V> f1,
          final Function<? super B, ? extends V> f2) {
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
}
