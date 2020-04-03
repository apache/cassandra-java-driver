/*
 * Copyright DataStax, Inc.
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

import jnr.ffi.Platform;

/** A gateway to perform system calls. */
public class Native {

  private static NativeImpl impl = new JnrNativeImpl();

  /** Whether {@link Native#currentTimeMicros()} is available on this system. */
  public static boolean isCurrentTimeMicrosAvailable() {
    return impl.gettimeofdayAvailable();
  }

  /**
   * The current time in microseconds, as returned by libc.gettimeofday(); can only be used if
   * {@link #isCurrentTimeMicrosAvailable()} is true.
   */
  public static long currentTimeMicros() {
    return impl.gettimeofday();
  }

  public static boolean isGetProcessIdAvailable() {
    return impl.getpidAvailable();
  }

  public static int getProcessId() {
    return impl.getpid();
  }

  /**
   * Returns {@code true} if JNR {@link Platform} class is loaded, and {@code false} otherwise.
   *
   * @return {@code true} if JNR {@link Platform} class is loaded.
   */
  public static boolean isPlatformAvailable() {
    return impl.cpuAvailable();
  }

  /**
   * Returns the current processor architecture the JVM is running on, as reported by {@link
   * Platform#getCPU()}.
   *
   * @return the current processor architecture.
   * @throws IllegalStateException if JNR Platform library is not loaded.
   */
  public static String getCPU() {
    return impl.cpu();
  }
}
