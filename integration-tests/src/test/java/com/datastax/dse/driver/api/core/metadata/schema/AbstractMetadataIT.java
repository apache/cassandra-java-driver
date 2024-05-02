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
package com.datastax.dse.driver.api.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Optional;
import org.junit.experimental.categories.Category;

/* Abstract class to hold common methods for Metadata Schema tests. */
@Category(ParallelizableTests.class)
public abstract class AbstractMetadataIT {

  /* Convenience method for executing a CQL statement using the test's Session Rule. */
  public void execute(String cql) {
    getSessionRule()
        .session()
        .execute(
            SimpleStatement.builder(cql)
                .setExecutionProfile(getSessionRule().slowProfile())
                .build());
  }

  /**
   * Convenience method for retrieving the Keyspace metadata from this test's Session Rule. Also
   * asserts the Keyspace exists and has the expected name.
   */
  public DseKeyspaceMetadata getKeyspace() {
    Optional<KeyspaceMetadata> keyspace =
        getSessionRule().session().getMetadata().getKeyspace(getSessionRule().keyspace());
    assertThat(keyspace)
        .isPresent()
        .hasValueSatisfying(
            ks -> {
              assertThat(ks).isInstanceOf(DseKeyspaceMetadata.class);
              assertThat(ks.getName()).isEqualTo(getSessionRule().keyspace());
            });
    return ((DseKeyspaceMetadata) keyspace.get());
  }

  /* Concrete ITs should return their ClassRule SessionRule. */
  protected abstract SessionRule<CqlSession> getSessionRule();
}
