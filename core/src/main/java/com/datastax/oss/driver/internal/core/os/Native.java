/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.os;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.annotations.Out;
import jnr.ffi.annotations.Transient;
import jnr.posix.POSIXFactory;
import jnr.posix.util.DefaultPOSIXHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A gateway to perform system calls. */
public class Native {

  private static final Logger LOG = LoggerFactory.getLogger(Native.class);

  /** Whether {@link Native#currentTimeMicros()} is available on this system. */
  public static boolean isCurrentTimeMicrosAvailable() {
    try {
      return LibCLoader.GET_TIME_OF_DAY_AVAILABLE;
    } catch (NoClassDefFoundError e) {
      return false;
    }
  }

  /**
   * The current time in microseconds, as returned by libc.gettimeofday(); can only be used if
   * {@link #isCurrentTimeMicrosAvailable()} is true.
   */
  public static long currentTimeMicros() {
    if (!isCurrentTimeMicrosAvailable()) {
      throw new IllegalStateException(
          "Native call not available. "
              + "Check isCurrentTimeMicrosAvailable() before calling this method.");
    }
    LibCLoader.Timeval tv = new LibCLoader.Timeval(LibCLoader.LIB_C_RUNTIME);
    int res = LibCLoader.LIB_C.gettimeofday(tv, null);
    if (res != 0) {
      throw new IllegalStateException("Call to libc.gettimeofday() failed with result " + res);
    }
    return tv.tv_sec.get() * 1000000 + tv.tv_usec.get();
  }

  public static boolean isGetProcessIdAvailable() {
    try {
      return PosixLoader.GET_PID_AVAILABLE;
    } catch (NoClassDefFoundError e) {
      return false;
    }
  }

  public static int getProcessId() {
    if (!isGetProcessIdAvailable()) {
      throw new IllegalStateException(
          "Native call not available. "
              + "Check isGetProcessIdAvailable() before calling this method.");
    }
    return PosixLoader.POSIX.getpid();
  }

  /**
   * If jnr-ffi is not in the classpath at runtime, we'll fail to initialize the static fields
   * below, but we still want {@link Native} to initialize successfully, so use an inner class.
   */
  private static class LibCLoader {

    /** Handles libc calls through JNR (must be public). */
    public interface LibC {
      int gettimeofday(@Out @Transient Timeval tv, Pointer unused);
    }

    // See http://man7.org/linux/man-pages/man2/settimeofday.2.html
    private static class Timeval extends Struct {
      private final time_t tv_sec = new time_t();
      private final Unsigned32 tv_usec = new Unsigned32();

      private Timeval(Runtime runtime) {
        super(runtime);
      }
    }

    private static final LibC LIB_C;
    private static final Runtime LIB_C_RUNTIME;
    private static final boolean GET_TIME_OF_DAY_AVAILABLE;

    static {
      LibC libc;
      Runtime runtime = null;
      try {
        libc = LibraryLoader.create(LibC.class).load("c");
        runtime = Runtime.getRuntime(libc);
      } catch (Throwable t) {
        libc = null;
        LOG.debug("Error loading libc", t);
      }
      LIB_C = libc;
      LIB_C_RUNTIME = runtime;
      boolean getTimeOfDayAvailable = false;
      if (LIB_C_RUNTIME != null) {
        try {
          getTimeOfDayAvailable = LIB_C.gettimeofday(new Timeval(LIB_C_RUNTIME), null) == 0;
        } catch (Throwable t) {
          LOG.debug("Error accessing libc.gettimeofday()", t);
        }
      }
      GET_TIME_OF_DAY_AVAILABLE = getTimeOfDayAvailable;
    }
  }

  /** @see LibCLoader */
  private static class PosixLoader {
    private static final jnr.posix.POSIX POSIX;
    private static final boolean GET_PID_AVAILABLE;

    static {
      jnr.posix.POSIX posix;
      try {
        posix = POSIXFactory.getPOSIX(new DefaultPOSIXHandler(), true);
      } catch (Throwable t) {
        posix = null;
        LOG.debug("Error loading POSIX", t);
      }
      POSIX = posix;
      boolean getPidAvailable = false;
      if (POSIX != null) {
        try {
          POSIX.getpid();
          getPidAvailable = true;
        } catch (Throwable t) {
          LOG.debug("Error accessing posix.getpid()", t);
        }
      }
      GET_PID_AVAILABLE = getPidAvailable;
    }
  }
}
