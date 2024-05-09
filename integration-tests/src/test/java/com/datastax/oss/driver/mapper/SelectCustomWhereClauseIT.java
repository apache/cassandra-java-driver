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
package com.datastax.oss.driver.mapper;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assume.assumeFalse;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "3.4",
    description = "Creates a SASI index")
public class SelectCustomWhereClauseIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductDao dao;

  @BeforeClass
  public static void setup() {
    // SASI index creation is broken in DSE 6.8.0
    // All tests in this class require SASI, so ensure it's working
    assumeFalse(InventoryITBase.isSasiBroken(CCM_RULE));

    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    InventoryMapper inventoryMapper =
        new SelectCustomWhereClauseIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(SESSION_RULE.keyspace());
    dao.save(FLAMETHROWER);
    dao.save(MP3_DOWNLOAD);
  }

  @Test
  public void should_select_with_custom_clause() {
    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(
            () -> {
              PagingIterable<Product> products = dao.findByDescription("%mp3%");
              assertThat(products.one()).isEqualTo(MP3_DOWNLOAD);
              assertThat(products.iterator()).isExhausted();
            });
  }

  @Test
  public void should_select_with_custom_clause_asynchronously() {
    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(
            () -> {
              MappedAsyncPagingIterable<Product> iterable =
                  CompletableFutures.getUninterruptibly(
                      dao.findByDescriptionAsync("%mp3%").toCompletableFuture());
              assertThat(iterable.one()).isEqualTo(MP3_DOWNLOAD);
              assertThat(iterable.currentPage().iterator()).isExhausted();
              assertThat(iterable.hasMorePages()).isFalse();
            });
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductDao {
    /** Note that this relies on a SASI index. */
    @Select(customWhereClause = "description LIKE :searchString")
    PagingIterable<Product> findByDescription(String searchString);

    /** Note that this relies on a SASI index. */
    @Select(customWhereClause = "description LIKE :\"Search String\"")
    CompletionStage<MappedAsyncPagingIterable<Product>> findByDescriptionAsync(
        @CqlName("\"Search String\"") String searchString);

    @Delete
    void delete(Product product);

    @Insert
    void save(Product product);
  }
}
