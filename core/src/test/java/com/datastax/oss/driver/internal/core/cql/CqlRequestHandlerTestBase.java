/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;

abstract class CqlRequestHandlerTestBase {

  protected static final DefaultSimpleStatement SIMPLE_STATEMENT =
      new DefaultSimpleStatement("mock query", Collections.emptyList(), null);

  @Mock protected Node node1;
  @Mock protected Node node2;
  @Mock protected Node node3;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  protected static Frame defaultFrameOf(Message responseMessage) {
    return Frame.forResponse(
        CoreProtocolVersion.V4.getCode(),
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
            new int[] {});
    Queue<List<ByteBuffer>> data = new LinkedList<>();
    data.add(ImmutableList.of(Bytes.fromHexString("0x68656C6C6F2C20776F726C64")));
    return new Rows(metadata, data);
  }
}
