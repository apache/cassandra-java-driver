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

import org.slf4j.Logger;

public class Loggers {

  /**
   * Emits a warning log that includes an exception. If the current level is debug, the full stack
   * trace is included, otherwise only the exception's message.
   */
  public static void warnWithException(Logger logger, String format, Object... arguments) {
    if (logger.isDebugEnabled()) {
      logger.warn(format, arguments);
    } else {
      Object last = arguments[arguments.length - 1];
      if (last instanceof Throwable) {
        arguments[arguments.length - 1] = ((Throwable) last).getMessage();
        logger.warn(format + " ({})", arguments);
      } else {
        // Should only be called with an exception as last argument, but handle gracefully anyway
        logger.warn(format, arguments);
      }
    }
  }
}
