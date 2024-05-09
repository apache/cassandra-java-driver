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
package com.datastax.oss.driver.internal.core.cql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public abstract class CqlRequestHandlerTestBase {

  protected static final SimpleStatement UNDEFINED_IDEMPOTENCE_STATEMENT =
      SimpleStatement.newInstance("mock query");
  protected static final SimpleStatement IDEMPOTENT_STATEMENT =
      SimpleStatement.builder("mock query").setIdempotence(true).build();
  protected static final SimpleStatement NON_IDEMPOTENT_STATEMENT =
      SimpleStatement.builder("mock query").setIdempotence(false).build();
  protected static final BatchStatement UNDEFINED_IDEMPOTENCE_BATCH_STATEMENT =
      BatchStatement.newInstance(BatchType.LOGGED, UNDEFINED_IDEMPOTENCE_STATEMENT);
  protected static final BatchStatement IDEMPOTENT_BATCH_STATEMENT =
      BatchStatement.newInstance(BatchType.LOGGED, IDEMPOTENT_STATEMENT).setIdempotent(true);
  protected static final BatchStatement NON_IDEMPOTENT_BATCH_STATEMENT =
      BatchStatement.newInstance(BatchType.LOGGED, NON_IDEMPOTENT_STATEMENT).setIdempotent(false);

  @Mock protected DefaultNode node1;
  @Mock protected DefaultNode node2;
  @Mock protected DefaultNode node3;
  @Mock protected NodeMetricUpdater nodeMetricUpdater1;
  @Mock protected NodeMetricUpdater nodeMetricUpdater2;
  @Mock protected NodeMetricUpdater nodeMetricUpdater3;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(node1.getMetricUpdater()).thenReturn(nodeMetricUpdater1);
    when(nodeMetricUpdater1.isEnabled(any(NodeMetric.class), anyString())).thenReturn(true);
    when(node2.getMetricUpdater()).thenReturn(nodeMetricUpdater2);
    when(nodeMetricUpdater2.isEnabled(any(NodeMetric.class), anyString())).thenReturn(true);
    when(node3.getMetricUpdater()).thenReturn(nodeMetricUpdater3);
    when(nodeMetricUpdater3.isEnabled(any(NodeMetric.class), anyString())).thenReturn(true);
  }

  protected static Frame defaultFrameOf(Message responseMessage) {
    return Frame.forResponse(
        DefaultProtocolVersion.V4.getCode(),
        0,
        null,
        Frame.NO_PAYLOAD,
        Collections.emptyList(),
        responseMessage);
  }

  // Returns a single row, with a single "message" column with the value "hello, world"
  protected static Message singleRow() {
    RowsMetadata metadata =
        new RowsMetadata(
            ImmutableList.of(
                new ColumnSpec(
                    "ks",
                    "table",
                    "message",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))),
            null,
            new int[] {},
            null);
    Queue<List<ByteBuffer>> data = new ArrayDeque<>();
    data.add(ImmutableList.of(Bytes.fromHexString("0x68656C6C6F2C20776F726C64")));
    return new DefaultRows(metadata, data);
  }

  /**
   * The combination of the default idempotence option and statement setting that produce an
   * idempotent statement.
   */
  @DataProvider
  public static Object[][] idempotentConfig() {
    return new Object[][] {
      new Object[] {true, UNDEFINED_IDEMPOTENCE_STATEMENT},
      new Object[] {false, IDEMPOTENT_STATEMENT},
      new Object[] {true, IDEMPOTENT_STATEMENT},
      new Object[] {true, UNDEFINED_IDEMPOTENCE_BATCH_STATEMENT},
      new Object[] {false, IDEMPOTENT_BATCH_STATEMENT},
      new Object[] {true, IDEMPOTENT_BATCH_STATEMENT},
    };
  }

  /**
   * The combination of the default idempotence option and statement setting that produce a non
   * idempotent statement.
   */
  @DataProvider
  public static Object[][] nonIdempotentConfig() {
    return new Object[][] {
      new Object[] {false, UNDEFINED_IDEMPOTENCE_STATEMENT},
      new Object[] {true, NON_IDEMPOTENT_STATEMENT},
      new Object[] {false, NON_IDEMPOTENT_STATEMENT},
      new Object[] {false, UNDEFINED_IDEMPOTENCE_BATCH_STATEMENT},
      new Object[] {true, NON_IDEMPOTENT_BATCH_STATEMENT},
      new Object[] {false, NON_IDEMPOTENT_BATCH_STATEMENT},
    };
  }

  @DataProvider
  public static Object[][] allIdempotenceConfigs() {
    return TestDataProviders.concat(idempotentConfig(), nonIdempotentConfig());
  }
}
