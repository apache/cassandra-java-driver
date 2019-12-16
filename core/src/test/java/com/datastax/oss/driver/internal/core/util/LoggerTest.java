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

import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

public class LoggerTest {
  public static LoggerSetup setupTestLogger(Class<?> clazz, Level levelToCapture) {
    @SuppressWarnings("unchecked")
    Appender<ILoggingEvent> appender = (Appender<ILoggingEvent>) mock(Appender.class);

    ArgumentCaptor<ILoggingEvent> loggingEventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
    Logger logger = (Logger) LoggerFactory.getLogger(clazz);
    Level originalLoggerLevel = logger.getLevel();
    logger.setLevel(levelToCapture);
    logger.addAppender(appender);
    return new LoggerSetup(appender, originalLoggerLevel, logger, loggingEventCaptor);
  }

  public static class LoggerSetup {

    private final Level originalLoggerLevel;
    public final Appender<ILoggingEvent> appender;
    public final Logger logger;
    public ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

    private LoggerSetup(
        Appender<ILoggingEvent> appender,
        Level originalLoggerLevel,
        Logger logger,
        ArgumentCaptor<ILoggingEvent> loggingEventCaptor) {
      this.appender = appender;
      this.originalLoggerLevel = originalLoggerLevel;
      this.logger = logger;
      this.loggingEventCaptor = loggingEventCaptor;
    }

    public void close() {
      logger.detachAppender(appender);
      logger.setLevel(originalLoggerLevel);
    }
  }
}
