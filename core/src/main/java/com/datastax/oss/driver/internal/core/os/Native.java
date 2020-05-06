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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A gateway to perform system calls. */
public class Native {

  private static final Logger LOG = LoggerFactory.getLogger(Native.class);

  private static class LibcLoader {

    /* These values come from Graal's imageinfo API which aims to offer the ability to detect
     * when we're in the Graal build/run time via system props.  The maintainers of Graal have
     * agreed that this API will not change over time.  We reference these props as literals
     * to avoid introducing a dependency on Graal code for non-Graal users here. */
    private static final String GRAAL_STATUS_PROP = "org.graalvm.nativeimage.imagecode";
    private static final String GRAAL_BUILDTIME_STATUS = "buildtime";
    private static final String GRAAL_RUNTIME_STATUS = "runtime";

    public Libc load() {
      try {
        if (isGraal()) {
          LOG.info("Using Graal-specific native functions");
          return new GraalLibc();
        }
        return new JnrLibc();
      } catch (Throwable t) {
        LOG.info(
            "Unable to load JNR native implementation. This could be normal if JNR is excluded from the classpath",
            t);
        return new EmptyLibc();
      }
    }

    private boolean isGraal() {

      String val = System.getProperty(GRAAL_STATUS_PROP);
      return val != null
          && (val.equals(GRAAL_RUNTIME_STATUS) || val.equalsIgnoreCase(GRAAL_BUILDTIME_STATUS));
    }
  }

  private static final Libc LIBC = new LibcLoader().load();
  private static final CpuInfo.Cpu CPU = CpuInfo.determineCpu();

  private static final String NATIVE_CALL_ERR_MSG = "Native call failed or was not available";

  /** Whether {@link Native#currentTimeMicros()} is available on this system. */
  public static boolean isCurrentTimeMicrosAvailable() {
    return LIBC.available();
  }

  /**
   * The current time in microseconds, as returned by libc.gettimeofday(); can only be used if
   * {@link #isCurrentTimeMicrosAvailable()} is true.
   */
  public static long currentTimeMicros() {
    return LIBC.gettimeofday().orElseThrow(() -> new IllegalStateException(NATIVE_CALL_ERR_MSG));
  }

  public static boolean isGetProcessIdAvailable() {
    return LIBC.available();
  }

  public static int getProcessId() {
    return LIBC.getpid().orElseThrow(() -> new IllegalStateException(NATIVE_CALL_ERR_MSG));
  }

  /**
   * Returns the current processor architecture the JVM is running on. This value should match up to
   * what's returned by jnr-ffi's Platform.getCPU() method.
   *
   * @return the current processor architecture.
   */
  public static String getCpu() {
    return CPU.toString();
  }
}
