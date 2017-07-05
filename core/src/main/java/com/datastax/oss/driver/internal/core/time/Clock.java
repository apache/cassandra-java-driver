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
package com.datastax.oss.driver.internal.core.time;

import com.datastax.oss.driver.internal.core.os.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small abstraction around system clock that aims to provide microsecond precision with the best
 * accuracy possible.
 */
public interface Clock {
  Logger LOG = LoggerFactory.getLogger(Clock.class);

  /** Returns the best implementation for the current platform. */
  static Clock getInstance(boolean forceJavaClock) {
    if (forceJavaClock) {
      LOG.info("Using Java system clock because this was explicitly required in the configuration");
      return new JavaClock();
    } else if (!Native.CURRENT_TIME_MICROS_AVAILABLE) {
      LOG.info(
          "Could not access native clock (see debug logs for details), "
              + "falling back to Java system clock");
      return new JavaClock();
    } else {
      LOG.info("Using native clock for microsecond precision");
      return new NativeClock();
    }
  }

  /**
   * Returns the difference, measured in <b>microseconds</b>, between the current time and and the
   * Epoch (that is, midnight, January 1, 1970 UTC).
   */
  long currentTimeMicros();
}
