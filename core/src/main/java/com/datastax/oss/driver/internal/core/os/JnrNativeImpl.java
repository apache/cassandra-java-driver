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

import java.util.Optional;
import java.util.function.Supplier;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import jnr.posix.Timeval;
import jnr.posix.util.DefaultPOSIXHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JnrNativeImpl implements NativeImpl {

  private static final Logger LOG = LoggerFactory.getLogger(JnrNativeImpl.class);

  private final Optional<POSIX> posix;

  public JnrNativeImpl() {

    this.posix = loadPosix();
  }

  private Optional<POSIX> loadPosix() {

    try {
      return Optional.of(POSIXFactory.getPOSIX(new DefaultPOSIXHandler(), true))
          .flatMap(this::validatePosix);
    } catch (Throwable t) {
      LOG.debug("Error loading POSIX", t);
      return Optional.empty();
    }
  }

  private Optional<POSIX> validatePosix(POSIX posix) {

    try {

      posix.getpid();
    } catch (Throwable t) {

      LOG.debug("Error calling getpid()", t);
      return Optional.empty();
    }

    try {

      Timeval tv = posix.allocateTimeval();
      int rv = posix.gettimeofday(tv);
      if (rv != 0) {

        LOG.debug("Expected getitimeofday() to return zero, observed {}", rv);
        return Optional.empty();
      }
    } catch (Throwable t) {

      LOG.debug("Error calling gettimeofday()", t);
      return Optional.empty();
    }

    return Optional.of(posix);
  }

  @Override
  public boolean gettimeofdayAvailable() {
    return posix.isPresent();
  }

  @Override
  public long gettimeofday() {

    Supplier<IllegalStateException> exceptionSupplier =
        () -> {
          throw new IllegalStateException(
              "Native call not available. "
                  + "Check isCurrentTimeMicrosAvailable() before calling this method.");
        };

    Timeval tv = this.posix.map(POSIX::allocateTimeval).orElseThrow(exceptionSupplier);
    int rv = this.posix.map(p -> p.gettimeofday(tv)).orElseThrow(exceptionSupplier);
    if (rv != 0) {
      throw new IllegalStateException(
          "Expected 0 return value from gettimeofday(), observed " + rv);
    }
    return tv.sec() * 1_000_000 + tv.usec();
  }

  @Override
  public boolean getpidAvailable() {
    return posix.isPresent();
  }

  @Override
  public int getpid() {
    return this.posix
        .map(POSIX::getpid)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Native call not available. "
                        + "Check isGetProcessIdAvailable() before calling this method."));
  }
}
