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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;

public class TestResponses {
  /** The response to the query run by each connection to check if the cluster name matches. */
  public static Rows clusterNameResponse(String actualClusterName) {
    ColumnSpec colSpec =
        new ColumnSpec(
            "system",
            "local",
            "cluster_name",
            0,
            RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR));
    RowsMetadata metadata = new RowsMetadata(ImmutableList.of(colSpec), null, null, null);
    Queue<List<ByteBuffer>> data = Lists.newLinkedList();
    data.add(Lists.newArrayList(ByteBuffer.wrap(actualClusterName.getBytes(Charsets.UTF_8))));
    return new DefaultRows(metadata, data);
  }
}
