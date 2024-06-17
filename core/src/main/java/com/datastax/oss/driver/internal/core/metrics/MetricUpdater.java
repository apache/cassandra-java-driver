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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * Note about profiles names: they are included to keep the possibility to break up metrics per
 * profile in the future, but right now the default updater implementations ignore them. The driver
 * internals provide a profile name when it makes sense and is practical; in other cases, it passes
 * {@code null}.
 */
public interface MetricUpdater<MetricT> {

  void incrementCounter(MetricT metric, @Nullable String profileName, long amount);

  default void incrementCounter(MetricT metric, @Nullable String profileName) {
    incrementCounter(metric, profileName, 1);
  }

  // note: currently unused
  void updateHistogram(MetricT metric, @Nullable String profileName, long value);

  void markMeter(MetricT metric, @Nullable String profileName, long amount);

  default void markMeter(MetricT metric, @Nullable String profileName) {
    markMeter(metric, profileName, 1);
  }

  void updateTimer(MetricT metric, @Nullable String profileName, long duration, TimeUnit unit);

  boolean isEnabled(MetricT metric, @Nullable String profileName);
}
