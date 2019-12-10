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
package com.datastax.oss.driver.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.concurrent.CompletionStage;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class DriverExecutionProfileCcmIT {

  @ClassRule public static final CcmRule CCM_RULE = CcmRule.getInstance();

  @Test
  public void should_use_profile_page_size() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 100)
            .startProfile("smallpages")
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 10)
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {

      CqlIdentifier keyspace = SessionUtils.uniqueKeyspaceId();
      DriverExecutionProfile slowProfile = SessionUtils.slowProfile(session);
      SessionUtils.createKeyspace(session, keyspace, slowProfile);

      session.execute(String.format("USE %s", keyspace.asCql(false)));

      // load 500 rows (value beyond page size).
      session.execute(
          SimpleStatement.builder(
                  "CREATE TABLE IF NOT EXISTS test (k int, v int, PRIMARY KEY (k,v))")
              .setExecutionProfile(slowProfile)
              .build());
      PreparedStatement prepared = session.prepare("INSERT INTO test (k, v) values (0, ?)");
      BatchStatementBuilder bs =
          BatchStatement.builder(DefaultBatchType.UNLOGGED).setExecutionProfile(slowProfile);
      for (int i = 0; i < 500; i++) {
        bs.addStatement(prepared.bind(i));
      }
      session.execute(bs.build());

      String query = "SELECT * FROM test where k=0";
      // Execute query without profile, should use global page size (100)
      CompletionStage<AsyncResultSet> future = session.executeAsync(query);
      AsyncResultSet result = CompletableFutures.getUninterruptibly(future);
      assertThat(result.remaining()).isEqualTo(100);
      result = CompletableFutures.getUninterruptibly(result.fetchNextPage());
      // next fetch should also be 100 pages.
      assertThat(result.remaining()).isEqualTo(100);

      // Execute query with profile, should use profile page size
      future =
          session.executeAsync(
              SimpleStatement.builder(query).setExecutionProfileName("smallpages").build());
      result = CompletableFutures.getUninterruptibly(future);
      assertThat(result.remaining()).isEqualTo(10);
      // next fetch should also be 10 pages.
      result = CompletableFutures.getUninterruptibly(result.fetchNextPage());
      assertThat(result.remaining()).isEqualTo(10);

      SessionUtils.dropKeyspace(session, keyspace, slowProfile);
    }
  }
}
