/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.adminrequest;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ThrottledAdminRequestHandler<ResultT> extends AdminRequestHandler<ResultT>
    implements Throttled {

  /**
   * @param shouldPreAcquireId whether to call {@link DriverChannel#preAcquireId()} before sending
   *     the request. This <b>must be false</b> if you obtained the connection from a pool ({@link
   *     ChannelPool#next()}, or {@link DefaultSession#getChannel(Node, String)}). It <b>must be
   *     true</b> if you are using a standalone channel (e.g. in {@link ControlConnection} or one of
   *     its auxiliary components).
   */
  public static ThrottledAdminRequestHandler<AdminResult> query(
      DriverChannel channel,
      boolean shouldPreAcquireId,
      Message message,
      Map<String, ByteBuffer> customPayload,
      Duration timeout,
      RequestThrottler throttler,
      SessionMetricUpdater metricUpdater,
      String logPrefix,
      String debugString) {
    return new ThrottledAdminRequestHandler<>(
        channel,
        shouldPreAcquireId,
        message,
        customPayload,
        timeout,
        throttler,
        metricUpdater,
        logPrefix,
        debugString,
        Rows.class);
  }

  /**
   * @param shouldPreAcquireId whether to call {@link DriverChannel#preAcquireId()} before sending
   *     the request. See {@link #query(DriverChannel, boolean, Message, Map, Duration,
   *     RequestThrottler, SessionMetricUpdater, String, String)} for more explanations.
   */
  public static ThrottledAdminRequestHandler<ByteBuffer> prepare(
      DriverChannel channel,
      boolean shouldPreAcquireId,
      Message message,
      Map<String, ByteBuffer> customPayload,
      Duration timeout,
      RequestThrottler throttler,
      SessionMetricUpdater metricUpdater,
      String logPrefix) {
    return new ThrottledAdminRequestHandler<>(
        channel,
        shouldPreAcquireId,
        message,
        customPayload,
        timeout,
        throttler,
        metricUpdater,
        logPrefix,
        message.toString(),
        Prepared.class);
  }

  private final long startTimeNanos;
  private final RequestThrottler throttler;
  private final SessionMetricUpdater metricUpdater;

  protected ThrottledAdminRequestHandler(
      DriverChannel channel,
      boolean preAcquireId,
      Message message,
      Map<String, ByteBuffer> customPayload,
      Duration timeout,
      RequestThrottler throttler,
      SessionMetricUpdater metricUpdater,
      String logPrefix,
      String debugString,
      Class<? extends Result> expectedResponseType) {
    super(
        channel,
        preAcquireId,
        message,
        customPayload,
        timeout,
        logPrefix,
        debugString,
        expectedResponseType);
    this.startTimeNanos = System.nanoTime();
    this.throttler = throttler;
    this.metricUpdater = metricUpdater;
  }

  @Override
  public CompletionStage<ResultT> start() {
    // Don't write request yet, wait for green light from throttler
    throttler.register(this);
    return result;
  }

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    if (wasDelayed) {
      metricUpdater.updateTimer(
          DefaultSessionMetric.THROTTLING_DELAY,
          null,
          System.nanoTime() - startTimeNanos,
          TimeUnit.NANOSECONDS);
    }
    super.start();
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    metricUpdater.incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, null);
    setFinalError(error);
  }

  @Override
  protected boolean setFinalResult(ResultT result) {
    boolean wasSet = super.setFinalResult(result);
    if (wasSet) {
      throttler.signalSuccess(this);
    }
    return wasSet;
  }

  @Override
  protected boolean setFinalError(Throwable error) {
    boolean wasSet = super.setFinalError(error);
    if (wasSet) {
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
    return wasSet;
  }
}
