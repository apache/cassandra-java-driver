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

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "3.6",
    description = "Uses UDT fields in IF conditions (CASSANDRA-7423)")
public class UpdateReactiveIT extends InventoryITBase {

  private static CcmRule ccmRule = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private static DseProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements(ccmRule)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    DseInventoryMapper dseInventoryMapper =
        new UpdateReactiveIT_DseInventoryMapperBuilder(session).build();
    dao = dseInventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void clearProductData() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE product")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
  }

  @Test
  public void should_update_entity_if_exists_reactive() {
    Flowable.fromPublisher(dao.updateReactive(FLAMETHROWER)).blockingSubscribe();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(FLAMETHROWER.getId())).blockingSingle())
        .isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    ReactiveResultSet rs = dao.updateIfExistsReactive(otherProduct);
    assertThat(Flowable.fromPublisher(rs).count().blockingGet()).isOne();
    assertThat(
            Single.fromPublisher(rs.getColumnDefinitions()).blockingGet().contains("description"))
        .isFalse();
    assertThat(Single.fromPublisher(rs.wasApplied()).blockingGet()).isTrue();
  }

  @Test
  public void should_update_entity_if_condition_is_met_reactive() {
    Flowable.fromPublisher(
            dao.updateReactive(
                new Product(
                    FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1))))
        .blockingSubscribe();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(FLAMETHROWER.getId())).blockingSingle())
        .isNotNull();
    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    ReactiveResultSet rs = dao.updateIfLengthReactive(otherProduct, 10);
    ReactiveRow row = Flowable.fromPublisher(rs).blockingSingle();
    assertThat(row.wasApplied()).isTrue();
    assertThat(row.getColumnDefinitions().contains("dimensions")).isFalse();
    assertThat(Single.fromPublisher(rs.getColumnDefinitions()).blockingGet().contains("dimensions"))
        .isFalse();
    assertThat(Single.fromPublisher(rs.wasApplied()).blockingGet()).isTrue();
  }

  @Test
  public void should_not_update_entity_if_condition_is_not_met_reactive() {
    Flowable.fromPublisher(
            dao.updateReactive(
                new Product(
                    FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1))))
        .blockingSubscribe();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(FLAMETHROWER.getId())).blockingSingle())
        .isNotNull()
        .extracting("description")
        .isEqualTo("Description for length 10");
    ReactiveResultSet rs =
        dao.updateIfLengthReactive(
            new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1)), 20);
    ReactiveRow row = Flowable.fromPublisher(rs).blockingSingle();
    assertThat(row.wasApplied()).isFalse();
    assertThat(row.getColumnDefinitions().contains("dimensions")).isTrue();
    assertThat(Single.fromPublisher(rs.getColumnDefinitions()).blockingGet().contains("dimensions"))
        .isTrue();
    assertThat(Single.fromPublisher(rs.wasApplied()).blockingGet()).isFalse();
  }

  @Mapper
  public interface DseInventoryMapper {

    @DaoFactory
    DseProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DseProductDao {

    @Update
    ReactiveResultSet updateReactive(Product product);

    @Update(ifExists = true)
    ReactiveResultSet updateIfExistsReactive(Product product);

    @Update(customIfClause = "dimensions.length = :length")
    ReactiveResultSet updateIfLengthReactive(Product product, int length);

    @Select
    MappedReactiveResultSet<Product> findByIdReactive(UUID productId);
  }
}
