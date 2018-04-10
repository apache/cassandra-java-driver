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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.DriverInfo;
import com.datastax.oss.driver.internal.core.os.Native;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SessionLifecycleListener} that logs messages when a {@linkplain Session session} is
 * initialized or closed.
 *
 * <p>It is recommended to register this listener before the session is created, by calling {@link
 * SessionBuilder#addSessionLifecycleListeners(SessionLifecycleListener...)}
 */
public class LoggingSessionLifecycleListener implements SessionLifecycleListener {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingSessionLifecycleListener.class);

  @VisibleForTesting static final AtomicInteger ONLY_ONCE = new AtomicInteger(0);

  @VisibleForTesting
  static final String INIT_START_TIME_KEY = "LoggingSessionLifecycleListener.INIT_START_TIME";

  @VisibleForTesting
  static final String CLOSE_START_TIME_KEY = "LoggingSessionLifecycleListener.CLOSE_START_TIME";

  /**
   * {@inheritDoc}
   *
   * <p>This implementation simply logs the name of the session being initialized, at level {@code
   * INFO}.
   *
   * <p>In addition, the very first time this method is invoked (JVM-wide), it will also log basic
   * driver information and the process ID, if available.
   */
  @Override
  public void beforeInit(Session session) {
    if (LOG.isInfoEnabled()) {
      session.getContext().getAttributes().putIfAbsent(INIT_START_TIME_KEY, System.nanoTime());
      if (ONLY_ONCE.incrementAndGet() == 1) {
        DriverInfo driverInfo = Session.getOssDriverInfo();
        if (Native.isGetProcessIdAvailable()) {
          LOG.info("{} loaded (PID: {})", driverInfo, Native.getProcessId());
        } else {
          LOG.info("{} loaded", driverInfo);
        }
      }
      LOG.info("[{}] Session initializing", session.getName());
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation simply logs the name of the session being initialized and the time
   * elapsed during initialization, at level {@code INFO}.
   */
  @Override
  public void afterInit(Session session) {
    if (LOG.isInfoEnabled()) {
      StringBuilder sb =
          new StringBuilder()
              .append("[")
              .append(session.getName())
              .append("] Session successfully initialized");
      Long start = (Long) session.getContext().getAttributes().remove(INIT_START_TIME_KEY);
      if (start != null) {
        sb.append(" in ").append(NanoTime.formatTimeSince(start));
      }
      if (session.getKeyspace() != null) {
        sb.append(", keyspace set to ").append(session.getKeyspace());
      }
      if (session instanceof DefaultSession) {
        int size = ((DefaultSession) session).getPools().size();
        sb.append(", connected to ").append(size).append(" node");
        if (size > 1) {
          sb.append('s');
        }
      }
      LOG.info(sb.toString());
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation simply logs the name of the session being initialized and the error
   * encountered, at level {@code ERROR}.
   */
  @Override
  public void onInitFailed(Session session, Throwable error) {
    if (LOG.isErrorEnabled()) {
      LOG.error(String.format("[%s] Session failed to initialize", session.getName()), error);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation simply logs the name of the session being shut down, at level {@code
   * INFO}.
   */
  @Override
  public void beforeClose(Session session) {
    if (LOG.isInfoEnabled()) {
      session.getContext().getAttributes().putIfAbsent(CLOSE_START_TIME_KEY, System.nanoTime());
      LOG.info("[{}] Session closing", session.getName());
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation simply logs the name of the session being shut down and the time elapsed
   * during shutdown, at level {@code INFO}.
   */
  @Override
  public void afterClose(Session session) {
    if (LOG.isInfoEnabled()) {
      StringBuilder sb =
          new StringBuilder()
              .append("[")
              .append(session.getName())
              .append("] Session successfully closed");
      Long start = (Long) session.getContext().getAttributes().remove(CLOSE_START_TIME_KEY);
      if (start != null) {
        sb.append(" in ").append(NanoTime.formatTimeSince(start));
      }
      LOG.info(sb.toString());
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation simply logs the name of the session being shut down and the error
   * encountered, at level {@code ERROR}.
   */
  @Override
  public void onCloseFailed(Session session, Throwable t) {
    if (LOG.isErrorEnabled()) {
      LOG.error(String.format("[%s] Session failed to close", session.getName()), t);
    }
  }
}
