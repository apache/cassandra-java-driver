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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A gateway to perform system calls. */
public class Native {

  private static final Logger LOG = LoggerFactory.getLogger(Native.class);

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

  /** Whether {@link Native#currentTimeMicros()} is available on this system. */
  public static final boolean CURRENT_TIME_MICROS_AVAILABLE;

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
    boolean gettimeofday = false;
    if (LIB_C_RUNTIME != null) {
      try {
        gettimeofday = LIB_C.gettimeofday(new Timeval(LIB_C_RUNTIME), null) == 0;
      } catch (Throwable t) {
        LOG.debug("Error accessing libc.gettimeofday()", t);
      }
    }
    CURRENT_TIME_MICROS_AVAILABLE = gettimeofday;
  }

  /**
   * The current time in microseconds, as returned by libc.gettimeofday(); can only be used if
   * {@link #CURRENT_TIME_MICROS_AVAILABLE} is true.
   */
  public static long currentTimeMicros() {
    if (!CURRENT_TIME_MICROS_AVAILABLE) {
      throw new IllegalStateException(
          "Native call not available. "
              + "Check CURRENT_TIME_MICROS_AVAILABLE before calling this method.");
    }
    Timeval tv = new Timeval(LIB_C_RUNTIME);
    int res = LIB_C.gettimeofday(tv, null);
    if (res != 0) {
      throw new IllegalStateException("Call to libc.gettimeofday() failed with result " + res);
    }
    return tv.tv_sec.get() * 1000000 + tv.tv_usec.get();
  }
}
