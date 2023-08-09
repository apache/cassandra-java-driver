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

import java.util.Locale;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraalLibc implements Libc {

  private static final Logger LOG = LoggerFactory.getLogger(GraalLibc.class);

  private static final Locale LOCALE = Locale.ENGLISH;

  private static final String MAC_PLATFORM_STR = "mac".toLowerCase(LOCALE);
  private static final String DARWIN_PLATFORM_STR = "darwin".toLowerCase(LOCALE);
  private static final String LINUX_PLATFORM_STR = "linux".toLowerCase(LOCALE);

  private final boolean available = checkAvailability();

  /* This method is adapted from of jnr.ffi.Platform.determineOS() in jnr-ffi version 2.1.10. **/
  private boolean checkPlatform() {

    String osName = System.getProperty("os.name").split(" ", -1)[0];
    String compareStr = osName.toLowerCase(Locale.ENGLISH);
    return compareStr.startsWith(MAC_PLATFORM_STR)
        || compareStr.startsWith(DARWIN_PLATFORM_STR)
        || compareStr.startsWith(LINUX_PLATFORM_STR);
  }

  private boolean checkAvailability() {

    if (!checkPlatform()) {
      return false;
    }

    try {
      getpidRaw();
    } catch (Throwable t) {

      LOG.debug("Error calling getpid()", t);
      return false;
    }

    try {
      gettimeofdayRaw();
    } catch (Throwable t) {

      LOG.debug("Error calling gettimeofday()", t);
      return false;
    }

    return true;
  }

  @Override
  public boolean available() {
    return this.available;
  }

  /* Substrate includes a substitution for Linux + Darwin which redefines System.nanoTime() to use
   * gettimeofday() (unless platform-specific higher-res clocks are available, which is even better). */
  @Override
  public Optional<Long> gettimeofday() {
    return this.available ? Optional.of(gettimeofdayRaw()) : Optional.empty();
  }

  private long gettimeofdayRaw() {
    return Math.round(System.nanoTime() / 1_000d);
  }

  @Override
  public Optional<Integer> getpid() {
    return this.available ? Optional.of(getpidRaw()) : Optional.empty();
  }

  private int getpidRaw() {
    return GraalGetpid.getpid();
  }
}
