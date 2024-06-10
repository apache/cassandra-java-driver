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
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import io.reactivex.Flowable;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class DeleteReactiveIT extends InventoryITBase {

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

    DseInventoryMapper inventoryMapper =
        new DeleteReactiveIT_DseInventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void insertFixtures() {
    Flowable.fromPublisher(dao.saveReactive(FLAMETHROWER)).blockingSubscribe();
  }

  @Test
  public void should_delete_entity_reactive() {
    UUID id = FLAMETHROWER.getId();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).blockingSingle()).isNotNull();

    ReactiveResultSet rs = dao.deleteEntityReactive(FLAMETHROWER);
    ReactiveRow row = Flowable.fromPublisher(rs).singleElement().blockingGet();

    assertThat(row).isNull();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).singleElement().blockingGet())
        .isNull();
  }

  @Test
  public void should_delete_by_id_reactive() {
    UUID id = FLAMETHROWER.getId();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).blockingSingle()).isNotNull();

    ReactiveResultSet rs = dao.deleteByIdReactive(id);
    ReactiveRow row = Flowable.fromPublisher(rs).singleElement().blockingGet();

    assertThat(row).isNull();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).singleElement().blockingGet())
        .isNull();

    // Non-existing id should be silently ignored
    rs = dao.deleteByIdReactive(id);
    row = Flowable.fromPublisher(rs).singleElement().blockingGet();

    assertThat(row).isNull();
  }

  @Test
  public void should_delete_if_exists_reactive() {
    UUID id = FLAMETHROWER.getId();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).blockingSingle()).isNotNull();
    {
      ReactiveResultSet rs = dao.deleteIfExistsReactive(FLAMETHROWER);
      ReactiveRow row = Flowable.fromPublisher(rs).blockingSingle();
      assertThat(row.wasApplied()).isTrue();
      assertThat(Flowable.fromPublisher(rs.wasApplied()).blockingSingle()).isTrue();
    }
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).singleElement().blockingGet())
        .isNull();
    {
      ReactiveResultSet rs = dao.deleteIfExistsReactive(FLAMETHROWER);
      ReactiveRow row = Flowable.fromPublisher(rs).singleElement().blockingGet();
      assertThat(row.wasApplied()).isFalse();
      assertThat(Flowable.fromPublisher(rs.wasApplied()).blockingSingle()).isFalse();
    }
  }

  @Test
  public void should_delete_with_condition_reactive() {
    UUID id = FLAMETHROWER.getId();
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).blockingSingle()).isNotNull();
    {
      ReactiveResultSet rs = dao.deleteIfDescriptionMatchesReactive(id, "foo");
      ReactiveRow row = Flowable.fromPublisher(rs).blockingSingle();
      assertThat(row.wasApplied()).isFalse();
      assertThat(Flowable.fromPublisher(rs.wasApplied()).blockingSingle()).isFalse();
      assertThat(row.getString("description")).isEqualTo(FLAMETHROWER.getDescription());
    }
    {
      ReactiveResultSet rs =
          dao.deleteIfDescriptionMatchesReactive(id, FLAMETHROWER.getDescription());
      ReactiveRow row = Flowable.fromPublisher(rs).blockingSingle();
      assertThat(row.wasApplied()).isTrue();
      assertThat(Flowable.fromPublisher(rs.wasApplied()).blockingSingle()).isTrue();
    }
    assertThat(Flowable.fromPublisher(dao.findByIdReactive(id)).singleElement().blockingGet())
        .isNull();
  }

  @Mapper
  public interface DseInventoryMapper {
    @DaoFactory
    DseProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DseProductDao {

    @Delete
    ReactiveResultSet deleteEntityReactive(Product product);

    @Delete(entityClass = Product.class)
    ReactiveResultSet deleteByIdReactive(UUID productId);

    @Delete(ifExists = true)
    ReactiveResultSet deleteIfExistsReactive(Product product);

    @Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
    ReactiveResultSet deleteIfDescriptionMatchesReactive(
        UUID productId, String expectedDescription);

    @Select
    MappedReactiveResultSet<Product> findByIdReactive(UUID productId);

    @Insert
    ReactiveResultSet saveReactive(Product product);
  }
}
