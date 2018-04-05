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
package com.datastax.oss.driver.internal.core.util;

public class NanoTime {

  private static final long ONE_HOUR = 3600L * 1000 * 1000 * 1000;
  private static final long ONE_MINUTE = 60L * 1000 * 1000 * 1000;
  private static final long ONE_SECOND = 1000 * 1000 * 1000;
  private static final long ONE_MILLISECOND = 1000 * 1000;
  private static final long ONE_MICROSECOND = 1000;

  /** Formats a duration in the best unit (truncating the fractional part). */
  public static String formatTimeSince(long startTimeNs) {
    return format(System.nanoTime() - startTimeNs);
  }

  /** Formats a duration in the best unit (truncating the fractional part). */
  public static String format(long elapsedNs) {
    if (elapsedNs >= ONE_HOUR) {
      long hours = elapsedNs / ONE_HOUR;
      long minutes = (elapsedNs % ONE_HOUR) / ONE_MINUTE;
      return hours + " h " + minutes + " mn";
    } else if (elapsedNs >= ONE_MINUTE) {
      long minutes = elapsedNs / ONE_MINUTE;
      long seconds = (elapsedNs % ONE_MINUTE) / ONE_SECOND;
      return minutes + " mn " + seconds + " s";
    } else if (elapsedNs >= ONE_SECOND) {
      long seconds = elapsedNs / ONE_SECOND;
      long milliseconds = (elapsedNs % ONE_SECOND) / ONE_MILLISECOND;
      return seconds + "." + milliseconds + " s";
    } else if (elapsedNs >= ONE_MILLISECOND) {
      return (elapsedNs / ONE_MILLISECOND) + " ms";
    } else if (elapsedNs >= ONE_MICROSECOND) {
      return (elapsedNs / ONE_MICROSECOND) + " us";
    } else {
      return elapsedNs + " ns";
    }
  }
}
