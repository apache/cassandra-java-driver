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
package com.datastax.oss.driver.example.guava.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.example.guava.api.GuavaSession;
import com.datastax.oss.driver.example.guava.api.GuavaSessionUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class GuavaSessionGetEntityIT extends CarITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static CarDao dao;

  private static GuavaSession newSession(CqlIdentifier keyspace) {
    return GuavaSessionUtils.builder()
        .addContactEndPoints(ccm.getContactPoints())
        .withKeyspace(keyspace)
        .build();
  }

  @BeforeClass
  public static void setup() {
    GuavaSession session = newSession(sessionRule.keyspace());

    for (String query : createStatements(ccm)) {
      try {
        session
            .executeAsync(
                SimpleStatement.builder(query)
                    .setExecutionProfile(sessionRule.slowProfile())
                    .build())
            .get();
      } catch (Exception e) {
        // shouldn't time out
      }
    }

    CarMapper carMapper = new GuavaSessionGetEntityIT_CarMapperBuilder(session).build();
    dao = carMapper.carDao(sessionRule.keyspace());

    dao.save(SAAB93);
    dao.save(SAAB900);
  }

  @Test
  public void should_get_entity_from_row() {
    GuavaSession session = newSession(sessionRule.keyspace());
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance("SELECT * FROM car WHERE id = ?", SAAB93.getId()),
            Statement.SYNC);
    Row row = rs.one();
    assertThat(row).isNotNull();

    Car car = dao.get(row);
    assertThat(car).isEqualTo(SAAB93);
  }

  @Test
  public void should_get_entity_from_first_row_of_result_set() {
    GuavaSession session = newSession(sessionRule.keyspace());
    ResultSet rs =
        session.execute(SimpleStatement.newInstance("SELECT * FROM car"), Statement.SYNC);

    Car car = dao.getOne(rs);
    // The order depends on the IDs, which are generated dynamically. This is good enough:
    assertThat(car.equals(SAAB93) || car.equals(SAAB900)).isTrue();
  }

  @Test
  public void should_get_entity_from_first_row_of_async_result_set() throws Exception {
    GuavaSession session = newSession(sessionRule.keyspace());
    AsyncResultSet rs = session.executeAsync("SELECT * FROM car").get();

    Car car = dao.getOne(rs);
    // The order depends on the IDs, which are generated dynamically. This is good enough:
    assertThat(car.equals(SAAB93) || car.equals(SAAB900)).isTrue();
  }

  @Test
  public void should_get_iterable_from_result_set() {
    GuavaSession session = newSession(sessionRule.keyspace());
    ResultSet rs =
        session.execute(SimpleStatement.newInstance("SELECT * FROM car"), Statement.SYNC);
    PagingIterable<Car> cars = dao.get(rs);
    assertThat(Sets.newHashSet(cars)).containsOnly(SAAB93, SAAB900);
  }

  @Test
  public void should_get_async_iterable_from_async_result_set() throws Exception {
    GuavaSession session = newSession(sessionRule.keyspace());
    AsyncResultSet rs = session.executeAsync("SELECT * FROM car").get();
    MappedAsyncPagingIterable<Car> cars = dao.get(rs);
    assertThat(Sets.newHashSet(cars.currentPage())).containsOnly(SAAB93, SAAB900);
    assertThat(cars.hasMorePages()).isFalse();
  }

  @Mapper
  public interface CarMapper {
    @DaoFactory
    CarDao carDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface CarDao {
    @GetEntity
    Car get(Row row);

    @GetEntity
    PagingIterable<Car> get(ResultSet resultSet);

    @GetEntity
    MappedAsyncPagingIterable<Car> get(AsyncResultSet resultSet);

    @GetEntity
    Car getOne(ResultSet resultSet);

    @GetEntity
    Car getOne(AsyncResultSet resultSet);

    @Insert
    void save(Car car);
  }
}
