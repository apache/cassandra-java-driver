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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import java.time.Duration;
import org.mockito.Mock;
import org.mockito.Mockito;

/**
 * Provides the environment to test a request handler, where a query plan can be defined, and the
 * behavior of each successive node simulated.
 */
public class GraphRequestHandlerTestHarness extends RequestHandlerTestHarness {

  @Mock DriverExecutionProfile testProfile;

  @Mock DriverExecutionProfile systemQueryExecutionProfile;

  protected GraphRequestHandlerTestHarness(Builder builder) {
    super(builder);

    // default graph options as in the reference.conf file
    Mockito.when(defaultProfile.getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null))
        .thenReturn("g");
    Mockito.when(defaultProfile.getString(DseDriverOption.GRAPH_SUB_PROTOCOL, "graphson-2.0"))
        .thenReturn("graphson-2.0");
    Mockito.when(defaultProfile.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false))
        .thenReturn(false);
    Mockito.when(defaultProfile.getString(DseDriverOption.GRAPH_NAME, null))
        .thenReturn("mockGraph");

    Mockito.when(testProfile.getName()).thenReturn("default");
    Mockito.when(testProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, null))
        .thenReturn(Duration.ofMillis(500L));
    Mockito.when(testProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.LOCAL_ONE.name());
    Mockito.when(testProfile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).thenReturn(5000);
    Mockito.when(testProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.SERIAL.name());
    Mockito.when(testProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE))
        .thenReturn(false);
    Mockito.when(testProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(true);
    Mockito.when(testProfile.getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null))
        .thenReturn("a");
    Mockito.when(testProfile.getString(DseDriverOption.GRAPH_SUB_PROTOCOL, "graphson-2.0"))
        .thenReturn("testMock");
    Mockito.when(testProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, null))
        .thenReturn(Duration.ofMillis(2));
    Mockito.when(testProfile.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false))
        .thenReturn(false);
    Mockito.when(testProfile.getString(DseDriverOption.GRAPH_NAME, null)).thenReturn("mockGraph");
    Mockito.when(testProfile.getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_TWO");
    Mockito.when(testProfile.getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_THREE");

    Mockito.when(config.getProfile("test-graph")).thenReturn(testProfile);

    Mockito.when(systemQueryExecutionProfile.getName()).thenReturn("default");
    Mockito.when(systemQueryExecutionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, null))
        .thenReturn(Duration.ofMillis(500L));
    Mockito.when(systemQueryExecutionProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.LOCAL_ONE.name());
    Mockito.when(systemQueryExecutionProfile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE))
        .thenReturn(5000);
    Mockito.when(
            systemQueryExecutionProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.SERIAL.name());
    Mockito.when(
            systemQueryExecutionProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE))
        .thenReturn(false);
    Mockito.when(systemQueryExecutionProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES))
        .thenReturn(true);
    Mockito.when(systemQueryExecutionProfile.getName()).thenReturn("graph-system-query");
    Mockito.when(systemQueryExecutionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, null))
        .thenReturn(Duration.ofMillis(2));
    Mockito.when(
            systemQueryExecutionProfile.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false))
        .thenReturn(true);
    Mockito.when(
            systemQueryExecutionProfile.getString(
                DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_TWO");
    Mockito.when(
            systemQueryExecutionProfile.getString(
                DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_THREE");

    Mockito.when(config.getProfile("graph-system-query")).thenReturn(systemQueryExecutionProfile);
  }

  public static GraphRequestHandlerTestHarness.Builder builder() {
    return new GraphRequestHandlerTestHarness.Builder();
  }

  public static class Builder extends RequestHandlerTestHarness.Builder {

    @Override
    public RequestHandlerTestHarness build() {
      return new GraphRequestHandlerTestHarness(this);
    }
  }
}
