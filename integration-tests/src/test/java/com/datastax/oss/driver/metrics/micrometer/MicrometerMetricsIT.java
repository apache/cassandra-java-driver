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
package com.datastax.oss.driver.metrics.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.metrics.common.AbstractMetricsTestBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.assertj.core.api.Condition;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class MicrometerMetricsIT extends AbstractMetricsTestBase {

  private static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();

  @Override
  protected void assertMetrics(CqlSession session) {
    await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(METER_REGISTRY.getMeters())
                    .haveExactly(
                        1,
                        buildTimerCondition(
                            "CQL_REQUESTS should be a SESSION Timer with count 10",
                            buildSessionMetricPattern(DefaultSessionMetric.CQL_REQUESTS, session),
                            a -> a == 10))
                    .haveExactly(
                        1,
                        buildTimerCondition(
                            "CQL_MESSAGESS should be a NODE Timer with count 10",
                            buildNodeMetricPattern(DefaultNodeMetric.CQL_MESSAGES, session),
                            a -> a == 10))
                    .haveExactly(
                        1,
                        buildGaugeCondition(
                            "CONNECTED_NODES should be a SESSION Gauge with count 1",
                            buildSessionMetricPattern(
                                DefaultSessionMetric.CONNECTED_NODES, session),
                            a -> a == 1))
                    .haveExactly(
                        1,
                        buildCounterCondition(
                            "RETRIES should be a NODE Counter with count 0",
                            buildNodeMetricPattern(DefaultNodeMetric.RETRIES, session),
                            a -> a == 0))
                    .haveExactly(
                        1,
                        buildCounterCondition(
                            "BYTES_SENT should be a SESSION Counter with count > 0",
                            buildSessionMetricPattern(DefaultSessionMetric.BYTES_SENT, session),
                            a -> a > 0))
                    .haveExactly(
                        1,
                        buildCounterCondition(
                            "BYTES_SENT should be a SESSION Counter with count > 0",
                            buildNodeMetricPattern(DefaultNodeMetric.BYTES_SENT, session),
                            a -> a > 0))
                    .haveExactly(
                        1,
                        buildCounterCondition(
                            "BYTES_RECEIVED should be a SESSION Counter with count > 0",
                            buildSessionMetricPattern(DefaultSessionMetric.BYTES_RECEIVED, session),
                            a -> a > 0))
                    .haveExactly(
                        1,
                        buildGaugeCondition(
                            "AVAILABLE_STREAMS should be a NODE Gauge with count 1024",
                            buildNodeMetricPattern(DefaultNodeMetric.AVAILABLE_STREAMS, session),
                            a -> a == 1024))
                    .haveExactly(
                        1,
                        buildCounterCondition(
                            "BYTES_RECEIVED should be a NODE Counter with count > 0",
                            buildNodeMetricPattern(DefaultNodeMetric.BYTES_RECEIVED, session),
                            a -> a > 0)));
  }

  @Override
  protected Object getMetricRegistry() {
    return METER_REGISTRY;
  }

  @Override
  protected String getMetricFactoryClass() {
    return "MicrometerMetricsFactory";
  }

  @Override
  protected Collection<?> getRegistryMetrics() {
    return METER_REGISTRY.getMeters();
  }

  private Condition<Meter> buildTimerCondition(
      String description, String metricPattern, Function<Long, Boolean> verifyFunction) {
    return new Condition<Meter>(description) {
      @Override
      public boolean matches(Meter obj) {
        if (!(obj instanceof Timer)) {
          return false;
        }
        Timer timer = (Timer) obj;
        return Pattern.matches(metricPattern, timer.getId().getName())
            && verifyFunction.apply(timer.count());
      }
    };
  }

  private Condition<Meter> buildCounterCondition(
      String description, String metricPattern, Function<Double, Boolean> verifyFunction) {
    return new Condition<Meter>(description) {
      @Override
      public boolean matches(Meter obj) {
        if (!(obj instanceof Counter)) {
          return false;
        }
        Counter counter = (Counter) obj;
        return Pattern.matches(metricPattern, counter.getId().getName())
            && verifyFunction.apply(counter.count());
      }
    };
  }

  private Condition<Meter> buildGaugeCondition(
      String description, String metricPattern, Function<Double, Boolean> verifyFunction) {
    return new Condition<Meter>(description) {
      @Override
      public boolean matches(Meter obj) {
        if (!(obj instanceof Gauge)) {
          return false;
        }
        Gauge gauge = (Gauge) obj;
        return Pattern.matches(metricPattern, gauge.getId().getName())
            && verifyFunction.apply(gauge.value());
      }
    };
  }
}
