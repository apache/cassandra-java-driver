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
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CassandraVersion(
    value = "4.0.0-alpha1",
    description = "Transient Replication is for Cassandra 4.0+")
@CCMConfig(config = "enable_transient_replication:true")
public class TransietReplicationTest extends CCMTestsSupport {

  private static final String TRANSIENT_REPLICATION_KEYSPACE = "transient_rep_ks";

  @BeforeClass(groups = "short")
  public void createKeyspace() {
    Map<String, Object> replicationOptions =
        ImmutableMap.<String, Object>of("class", "SimpleStrategy", "replication_factor", "3/1");

    // create keyspace
    session()
        .execute(
            SchemaBuilder.createKeyspace(TRANSIENT_REPLICATION_KEYSPACE)
                .with()
                .replication(replicationOptions));

    // verify the replication factor in the metadata
    assertThat(cluster().getMetadata().getKeyspace(TRANSIENT_REPLICATION_KEYSPACE).getReplication())
        .containsEntry("replication_factor", "3/1");
  }

  @AfterClass(groups = "short")
  public void dropKeyspace() {
    session().execute("drop keyspace " + TRANSIENT_REPLICATION_KEYSPACE);
  }

  @Test(groups = "short")
  public void should_handle_read_reapir_none() {
    // create a table with read_repair set to 'NONE'
    Session session = cluster().connect(TRANSIENT_REPLICATION_KEYSPACE);
    session.execute(
        SchemaBuilder.createTable("valid_table")
            .addPartitionKey("pk", DataType.text())
            .addColumn("data", DataType.text())
            .withOptions()
            .readRepair(TableOptions.ReadRepairValue.NONE));
    TableOptionsMetadata options =
        cluster()
            .getMetadata()
            .getKeyspace(TRANSIENT_REPLICATION_KEYSPACE)
            .getTable("valid_table")
            .getOptions();

    assertThat(options.getReadRepair()).isEqualTo("NONE");
    // assert that the default additional_write_policy exists as well
    assertThat(options.getAdditionalWritePolicy()).isEqualTo("99p");
  }

  @Test(groups = "short")
  public void should_fail_to_create_table_with_read_repair_blocking() {
    /**
     * Attempt to create a table with the default read_repair 'BLOCKING'. This should fail when the
     * keyspace uses transient replicas.
     */
    try {
      Session session = cluster().connect(TRANSIENT_REPLICATION_KEYSPACE);
      session.execute(
          SchemaBuilder.createTable("invalid_table")
              .addPartitionKey("pk", DataType.text())
              .addColumn("data", DataType.text()));
      fail(
          "Create table with default read_repair ('BLOCKING') is not supported when keyspace uses transient replicas.");
    } catch (InvalidQueryException iqe) {
      assertThat(iqe)
          .hasMessageContaining(
              "read_repair must be set to 'NONE' for transiently replicated keyspaces");
    }
  }
}
