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
package com.datastax.oss.driver.internal.core.metrics;

import java.util.concurrent.TimeUnit;

public interface MetricUpdater<MetricT> {

  void incrementCounter(MetricT metric, long amount);

  default void incrementCounter(MetricT metric) {
    incrementCounter(metric, 1);
  }

  void updateHistogram(MetricT metric, long value);

  void markMeter(MetricT metric, long amount);

  default void markMeter(MetricT metric) {
    markMeter(metric, 1);
  }

  void updateTimer(MetricT metric, long duration, TimeUnit unit);
}
