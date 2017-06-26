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
// @author: renar
//
// Date: Jan 27, 2015
// ---------------------

package com.ibm.async_util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

/**
 * A utility class which provides certain functionality that may not be portable (e.g.
 * {@link sun.misc.Unsafe}). Provides automatic fallback to portable alternatives when necessary
 */
@SuppressWarnings("restriction")
final class PlatformDependent {
  private PlatformDependent() {}

  // TODO(java9) VarHandles are supposed to provide fences without accessing Unsafe

  /**
   * @see sun.misc.Unsafe#loadFence()
   * @return true iff this method is supported
   */
  public static boolean loadFence() {
    return AlternativeHolder.UNSAFE_PROVIDER.loadFence();
  }

  /**
   * @see sun.misc.Unsafe#storeFence()
   * @return true iff this method is supported
   */
  public static boolean storeFence() {
    return AlternativeHolder.UNSAFE_PROVIDER.storeFence();
  }

  /**
   * @see sun.misc.Unsafe#fullFence()
   * @return true iff this method is supported
   */
  public static boolean fullFence() {
    return AlternativeHolder.UNSAFE_PROVIDER.fullFence();
  }

  private interface UnsafeProvider {
    public boolean loadFence();

    public boolean storeFence();

    public boolean fullFence();
  }

  private static UnsafeProvider safeJavaImpl() {
    return AlternativeHolder.PureJavaAlternative.INSTANCE;
  }

  /**
   * Provides alternatives implementations of a particular platform dependent facility. Uses
   * reflection to gracefully fall back to the portable Java implementation if the respective
   * platform-dependent one isn't available.
   */
  private static class AlternativeHolder {
    static final String UNSAFE_ALTERNATIVE_NAME = AlternativeHolder.class.getName()
        + "$UnsafeAlternative";

    /**
     * The provider for methods which use {@link sun.misc.Unsafe} (or java fallbacks)
     */
    static final UnsafeProvider UNSAFE_PROVIDER = AlternativeHolder.getUnsafeProvider();

    /**
     * Returns the Unsafe-using Comparer, or falls back to the pure-Java implementation if unable to
     * do so.
     */
    static UnsafeProvider getUnsafeProvider() {
      try {
        final Class<?> theClass = Class.forName(UNSAFE_ALTERNATIVE_NAME);

        final UnsafeProvider comparer = (UnsafeProvider) theClass.getEnumConstants()[0];
        return comparer;
      } catch (final Throwable t) { // ensure we really catch *everything*
        return PlatformDependent.safeJavaImpl();
      }
    }

    private enum PureJavaAlternative
        implements UnsafeProvider {
      INSTANCE;

      @Override
      public boolean loadFence() {
        return false;
      }

      @Override
      public boolean storeFence() {
        return false;
      }

      @Override
      public boolean fullFence() {
        return false;
      }
    }

    @SuppressWarnings({"unused"})
    // used via reflection
    private enum UnsafeAlternative
        implements UnsafeProvider {
      INSTANCE;

      private static final Unsafe unsafe;

      static {
        unsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            try {
              final Field f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return f.get(null);
            } catch (final NoSuchFieldException e) {
              // It doesn't matter what we throw;
              // it's swallowed in getBestComparer().
              throw new Error(e);
            } catch (final IllegalAccessException e) {
              throw new Error(e);
            }
          }
        });

        // sanity check - this should never fail
        if (unsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      @Override
      public boolean loadFence() {
        unsafe.loadFence();
        return true;
      }

      @Override
      public boolean storeFence() {
        unsafe.storeFence();
        return true;
      }

      @Override
      public boolean fullFence() {
        unsafe.fullFence();
        return true;
      }
    }
  }
}
