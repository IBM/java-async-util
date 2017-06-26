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
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;


public class BiasedEithers {

  /**
   * Left Projection of an {@link Either}, provides convenience methods that operate only on the
   * left element, if present.
   */
  public interface LeftEither<LEFT, RIGHT> {

    /**
     * Get the {@link Either} associated with this {@link LeftEither}
     */
    default Either<LEFT, RIGHT> either() {
      return map(Function.identity());
    }

    /**
     * Returns false if Right or returns the result of the application of f to the Left value
     *
     * @return the result of f applied to L or false
     */
    default boolean exists(final Function<? super LEFT, Boolean> func) {
      return map(func::apply).left().orElse(false);
    }

    /**
     * Returns the value from this Left otherwise throws NoSuchElementException if this is a Right
     * 
     * @return the contained value
     * @throws java.util.NoSuchElementException if this is a Right
     * 
     * @see LeftEither#orElse(Object)
     * @see LeftEither#toOptional()
     */
    default LEFT get() {
      return toOptional().get();
    }

    /**
     * Returns the value from this Left or defaultValue if this is a Right
     * 
     * @see LeftEither#get()
     * @see LeftEither#toOptional()
     */
    default LEFT orElse(final LEFT defaultValue) {
      return toOptional().orElse(defaultValue);
    }

    /**
     * Possibly return the contained value
     * 
     * @return Optional.of(left) if this is a Left. Optional.empty() if this is a Right
     * 
     * @see LeftEither#get()
     * @see LeftEither#orElse(Object)
     */
    default Optional<LEFT> toOptional() {
      return either().fold(left -> {
        if (left == null) {
          throw new NullPointerException("toOptional is not valid for null values");
        } else {
          return Optional.of(left);
        }
      }, left -> Optional.empty());
    }

    /**
     * Returns the Left value or the application of f to Right value
     */
    default LEFT applyOrGet(final Function<RIGHT, LEFT> f2) {
      return either().fold(left -> left, f2::apply);
    }

    /**
     * Map f through Left
     */
    default <RESULT> Either<RESULT, RIGHT> map(
        final Function<? super LEFT, ? extends RESULT> func) {
      return flatMap(element -> Either.left(func.apply(element)));
    }

    /**
     * If this is a Left, apply the given consumer to this Left
     */
    default void consume(final Consumer<? super LEFT> func) {
      flatMap(element -> {
        func.accept(element);
        return null;
      });
    }

    /**
     * Bind the f across Left
     */
    <A2, B2> Either<A2, B2> flatMap(Function<? super LEFT, ? extends Either<A2, B2>> func);


    static <A, B> LeftEither<A, B> left(final A a) {
      return new LeftEither<A, B>() {

        @Override
        public <A2, B2> Either<A2, B2> flatMap(
            final Function<? super A, ? extends Either<A2, B2>> func) {
          return func.apply(a);
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

    static <A, B> LeftEither<A, B> right(final B b) {
      return new LeftEither<A, B>() {

        @SuppressWarnings("unchecked")
        @Override
        public <A2, B2> Either<A2, B2> flatMap(
            final Function<? super A, ? extends Either<A2, B2>> func) {
          return (Either<A2, B2>) Either.right(b);
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
     * Flatten an Either of an Either and a right into an Either.
     */
    static <LEFT, RIGHT> Either<LEFT, RIGHT> flatten(
        final Either<Either<LEFT, RIGHT>, RIGHT> either) {
      return either.fold(element -> element, Either::right);
    }

    /**
     * Collect all Left values or the first Right encountered
     */
    @SuppressWarnings("unchecked")
    static <LEFT, RIGHT, INTERMEDIATE, RESULT> Either<RESULT, RIGHT> collect(
        final Collection<? extends Either<LEFT, RIGHT>> elements,
        final Collector<LEFT, INTERMEDIATE, RESULT> collector) {
      final INTERMEDIATE mutable = collector.supplier().get();
      final boolean finish =
          collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH);
      return elements.stream()
          .reduce(Either.<INTERMEDIATE, RIGHT>left(mutable), (reduction, element) -> {
            if (!reduction.isLeft()) {
              return reduction;
            } else if (element.isLeft()) {
              final INTERMEDIATE accumulated = reduction.left().get();
              final LEFT value = element.left().get();
              collector.accumulator().accept(accumulated, value);
              return reduction;
            } else {
              return element.left().map(ignore -> mutable);
            }
          }, (either1, either2) -> either1).left()
          .map(toFinalize -> finish ? (RESULT) toFinalize : collector.finisher().apply(toFinalize));
    }
  }

  /**
   * Right Projection of an {@link Either}, provides convenience methods that operate only on the
   * right element, if present.
   */
  public interface RightEither<LEFT, RIGHT> {

    /**
     * Get the {@link Either} associated with this {@link RightEither}
     */
    default Either<LEFT, RIGHT> either() {
      return map(Function.identity());
    }

    /**
     * Returns false if Left or returns the result of the application of f to the Right value
     *
     * @return the result of f applied to R or false
     */
    default boolean exists(final Function<? super RIGHT, Boolean> func) {
      return map(func::apply).right().orElse(false);
    }

    /**
     * Returns the value from this Right otherwise throws NoSuchElementException if this is a Left
     * 
     * @return the contained value
     * @throws java.util.NoSuchElementException if this is a Left
     * 
     * @see RightEither#orElse(Object)
     * @see RightEither#toOptional()
     */
    default RIGHT get() {
      return toOptional().get();
    }

    /**
     * Returns the value from this Right or defaultValue if this is a Left
     * 
     * @see RightEither#get()
     * @see RightEither#toOptional()
     */
    default RIGHT orElse(final RIGHT defaultValue) {
      return toOptional().orElse(defaultValue);
    }

    /**
     * Possibly return the contained value
     * 
     * @return Optional.of(right) if this is a Right. Optional.empty() if this is a Left
     * @throws NullPointerException if this is a Left and the value is null
     * 
     * @see RightEither#get()
     * @see RightEither#orElse(Object)
     */
    default Optional<RIGHT> toOptional() {
      return either().fold(right -> Optional.empty(), right -> {
        if (right == null) {
          throw new NullPointerException("toOptional is not valid for null values");
        } else {
          return Optional.of(right);
        }
      });
    }

    /**
     * Returns the Right value or the application of f to Left value
     */
    default RIGHT applyOrGet(final Function<LEFT, RIGHT> f1) {
      return either().fold(f1::apply, right -> right);
    }

    /**
     * Map f through Right
     */
    default <RESULT> Either<LEFT, RESULT> map(
        final Function<? super RIGHT, ? extends RESULT> func) {
      return flatMap(element -> Either.right(func.apply(element)));
    }

    /**
     * If this is a Right, apply the given consumer to this Right
     */
    default void consume(final Consumer<? super RIGHT> func) {
      flatMap(element -> {
        func.accept(element);
        return null;
      });
    }

    /**
     * Bind the f across Right
     */
    <A2, B2> Either<A2, B2> flatMap(Function<? super RIGHT, ? extends Either<A2, B2>> func);


    static <A, B> RightEither<A, B> left(final A a) {
      return new RightEither<A, B>() {

        @SuppressWarnings("unchecked")
        @Override
        public <A2, B2> Either<A2, B2> flatMap(
            final Function<? super B, ? extends Either<A2, B2>> func) {
          return (Either<A2, B2>) Either.left(a);
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

    static <A, B> RightEither<A, B> right(final B b) {
      return new RightEither<A, B>() {

        @Override
        public <A2, B2> Either<A2, B2> flatMap(
            final Function<? super B, ? extends Either<A2, B2>> func) {
          return func.apply(b);
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
     * Flatten an Either of an Either and a left into an Either.
     */
    static <LEFT, RIGHT> Either<LEFT, RIGHT> flatten(
        final Either<LEFT, Either<LEFT, RIGHT>> either) {
      return either.fold(Either::left, element -> element);
    }

    /**
     * Collect all Right values or the first Left encountered
     */
    @SuppressWarnings("unchecked")
    static <LEFT, RIGHT, INTERMEDIATE, RESULT> Either<LEFT, RESULT> collect(
        final Collection<? extends Either<LEFT, RIGHT>> elements,
        final Collector<RIGHT, INTERMEDIATE, RESULT> collector) {
      final INTERMEDIATE mutable = collector.supplier().get();
      final boolean finish =
          collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH);
      return elements.stream()
          .reduce(Either.<LEFT, INTERMEDIATE>right(mutable), (reduction, element) -> {
            if (!reduction.isRight()) {
              return reduction;
            } else if (element.isRight()) {
              final INTERMEDIATE accumulated = reduction.right().get();
              final RIGHT value = element.right().get();
              collector.accumulator().accept(accumulated, value);
              return reduction;
            } else {
              return element.right().map(ignore -> mutable);
            }
          }, (either1, either2) -> either1).right()
          .map(toFinalize -> finish ? (RESULT) toFinalize : collector.finisher().apply(toFinalize));
    }
  }
}
