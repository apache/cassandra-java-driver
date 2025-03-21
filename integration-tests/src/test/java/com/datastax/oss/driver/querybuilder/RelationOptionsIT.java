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
package com.datastax.oss.driver.querybuilder;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class RelationOptionsIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  private SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Rule public TestName name = new TestName();

  @Test
  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "3.0",
      description = "CRC check chance was moved to top level table in Cassandra 3.0")
  public void should_create_table_with_crc_check_chance() {
    sessionRule
        .session()
        .execute(
            SchemaBuilder.createTable(name.getMethodName())
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("name", DataTypes.TEXT)
                .withColumn("age", DataTypes.INT)
                .withCRCCheckChance(0.8)
                .build());
    KeyspaceMetadata keyspaceMetadata =
        sessionRule
            .session()
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .orElseThrow(AssertionError::new);
    String describeOutput = keyspaceMetadata.describeWithChildren(true).trim();

    assertThat(describeOutput).contains("crc_check_chance = 0.8");
  }

  @Test
  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "5.0",
      description = "chunk_length_kb was renamed to chunk_length_in_kb in Cassandra 5.0")
  public void should_create_table_with_chunk_length_in_kb() {
    sessionRule
        .session()
        .execute(
            SchemaBuilder.createTable(name.getMethodName())
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("name", DataTypes.TEXT)
                .withColumn("age", DataTypes.INT)
                .withLZ4Compression(4096)
                .build());
    KeyspaceMetadata keyspaceMetadata =
        sessionRule
            .session()
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .orElseThrow(AssertionError::new);
    String describeOutput = keyspaceMetadata.describeWithChildren(true).trim();

    assertThat(describeOutput).contains("'class':'org.apache.cassandra.io.compress.LZ4Compressor'");
    assertThat(describeOutput).contains("'chunk_length_in_kb':'4096'");
  }

  @Test
  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "3.0",
      maxExclusive = "5.0",
      description =
          "Deprecated compression options should still work with Cassandra >= 3.0 & < 5.0")
  public void should_create_table_with_deprecated_options() {
    sessionRule
        .session()
        .execute(
            SchemaBuilder.createTable(name.getMethodName())
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("name", DataTypes.TEXT)
                .withColumn("age", DataTypes.INT)
                .withLZ4Compression(4096, 0.8)
                .build());
    KeyspaceMetadata keyspaceMetadata =
        sessionRule
            .session()
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .orElseThrow(AssertionError::new);
    String describeOutput = keyspaceMetadata.describeWithChildren(true).trim();

    assertThat(describeOutput).contains("'class':'org.apache.cassandra.io.compress.LZ4Compressor'");
    assertThat(describeOutput).contains("'chunk_length_in_kb':'4096'");
    assertThat(describeOutput).contains("crc_check_chance = 0.8");
  }
}
