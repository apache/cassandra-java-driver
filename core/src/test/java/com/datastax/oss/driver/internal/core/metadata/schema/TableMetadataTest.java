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
package com.datastax.oss.driver.internal.core.metadata.schema;

import static com.datastax.oss.driver.api.core.CqlIdentifier.fromCql;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants.DataType;
import com.google.common.collect.ImmutableList;
import java.util.UUID;
import org.junit.Test;

public class TableMetadataTest {

  /** Tests CASSJAVA-2 */
  @Test
  public void should_describe_table_with_vector_correctly() {
    TableMetadata tableMetadata =
        new DefaultTableMetadata(
            fromCql("ks"),
            fromCql("tb"),
            UUID.randomUUID(),
            false,
            false,
            ImmutableList.of(
                new DefaultColumnMetadata(
                    fromCql("ks"),
                    fromCql("ks"),
                    fromCql("tb"),
                    new PrimitiveType(DataType.ASCII),
                    false)),
            ImmutableMap.of(),
            ImmutableMap.of(
                fromCql("a"),
                new DefaultColumnMetadata(
                    fromCql("ks"),
                    fromCql("ks"),
                    fromCql("tb"),
                    new DefaultVectorType(new PrimitiveType(DataType.INT), 3),
                    false)),
            ImmutableMap.of(),
            ImmutableMap.of());

    String describe1 = tableMetadata.describe(true);

    assertThat(describe1).contains("vector<int, 3>,");
  }
}
