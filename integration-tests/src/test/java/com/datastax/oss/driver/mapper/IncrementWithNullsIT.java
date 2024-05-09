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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Increment;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.mapper.IncrementIT.ProductRating;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "2.2")
public class IncrementWithNullsIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductRatingDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_rating(product_id uuid PRIMARY KEY, "
                    + "one_star counter, two_star counter, three_star counter, "
                    + "four_star counter, five_star counter)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    InventoryMapper inventoryMapper =
        new IncrementWithNullsIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productRatingDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_increment_counters() {
    UUID productId1 = UUID.randomUUID();
    UUID productId2 = UUID.randomUUID();

    dao.increment(productId1, null, null, null, null, 1L);
    dao.increment(productId1, null, null, null, null, 1L);
    dao.increment(productId1, null, null, null, 1L, null);

    dao.increment(productId2, null, 1L, null, null, null);
    dao.increment(productId2, null, null, 1L, null, null);
    dao.increment(productId2, 1L, null, null, null, null);

    ProductRating product1Totals = dao.get(productId1);
    assertThat(product1Totals.getFiveStar()).isEqualTo(2);
    assertThat(product1Totals.getFourStar()).isEqualTo(1);
    assertThat(product1Totals.getThreeStar()).isEqualTo(0);
    assertThat(product1Totals.getTwoStar()).isEqualTo(0);
    assertThat(product1Totals.getOneStar()).isEqualTo(0);

    ProductRating product2Totals = dao.get(productId2);
    assertThat(product2Totals.getFiveStar()).isEqualTo(0);
    assertThat(product2Totals.getFourStar()).isEqualTo(0);
    assertThat(product2Totals.getThreeStar()).isEqualTo(1);
    assertThat(product2Totals.getTwoStar()).isEqualTo(1);
    assertThat(product2Totals.getOneStar()).isEqualTo(1);
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductRatingDao productRatingDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.DO_NOT_SET)
  public interface ProductRatingDao {
    @Select
    ProductRating get(UUID productId);

    @Increment(entityClass = ProductRating.class)
    void increment(
        UUID productId, Long oneStar, Long twoStar, Long threeStar, Long fourStar, Long fiveStar);
  }
}
