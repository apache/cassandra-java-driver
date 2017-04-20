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
package com.datastax.oss.driver.internal.core.util;

public class NanoTime {

  private static final long ONE_HOUR = 3600L * 1000 * 1000 * 1000;
  private static final long ONE_MINUTE = 60L * 1000 * 1000 * 1000;
  private static final long ONE_SECOND = 1000 * 1000 * 1000;
  private static final long ONE_MILLISECOND = 1000 * 1000;
  private static final long ONE_MICROSECOND = 1000;

  /** Formats a duration in the best unit (truncating the fractional part). */
  public static String formatTimeSince(long startTimeNs) {
    long delta = System.nanoTime() - startTimeNs;
    if (delta >= ONE_HOUR) {
      return (delta / ONE_HOUR) + " h";
    } else if (delta >= ONE_MINUTE) {
      return (delta / ONE_MINUTE) + " mn";
    } else if (delta >= ONE_SECOND) {
      return (delta / ONE_SECOND) + " s";
    } else if (delta >= ONE_MILLISECOND) {
      return (delta / ONE_MILLISECOND) + " ms";
    } else if (delta >= ONE_MICROSECOND) {
      return (delta / ONE_MICROSECOND) + " us";
    } else {
      return delta + " ns";
    }
  }
}
