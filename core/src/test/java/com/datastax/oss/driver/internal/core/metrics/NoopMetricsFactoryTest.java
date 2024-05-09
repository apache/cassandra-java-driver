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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class NoopMetricsFactoryTest {

  @Test
  public void should_log_warning_when_metrics_enabled() {
    // given
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(context.getSessionName()).thenReturn("MockSession");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED))
        .thenReturn(Collections.singletonList(DefaultSessionMetric.CQL_REQUESTS.getPath()));
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(NoopMetricsFactory.class, Level.WARN);

    // when
    new NoopMetricsFactory(context);

    // then
    verify(logger.appender, times(1)).doAppend(logger.loggingEventCaptor.capture());
    assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
    assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
        .contains("[MockSession] Some session-level or node-level metrics were enabled");
  }
}
