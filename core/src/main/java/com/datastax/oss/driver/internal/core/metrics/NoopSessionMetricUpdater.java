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
package com.datastax.oss.driver.internal.core.metrics;

import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class NoopSessionMetricUpdater implements SessionMetricUpdater {

  public static NoopSessionMetricUpdater INSTANCE = new NoopSessionMetricUpdater();

  private NoopSessionMetricUpdater() {}

  @Override
  public void incrementCounter(SessionMetric metric, String profileName, long amount) {
    // nothing to do
  }

  @Override
  public void updateHistogram(SessionMetric metric, String profileName, long value) {
    // nothing to do
  }

  @Override
  public void markMeter(SessionMetric metric, String profileName, long amount) {
    // nothing to do
  }

  @Override
  public void updateTimer(SessionMetric metric, String profileName, long duration, TimeUnit unit) {
    // nothing to do
  }

  @Override
  public boolean isEnabled(SessionMetric metric, String profileName) {
    // since methods don't do anything, return false
    return false;
  }

  @Override
  public void clearMetrics() {}
}
