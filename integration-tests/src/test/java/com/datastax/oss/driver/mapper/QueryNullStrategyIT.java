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
package com.datastax.oss.driver.mapper;

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;
import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.SET_TO_NULL;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.function.BiConsumer;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** Covers null saving strategies in {@link Query} methods. */
@Category(ParallelizableTests.class)
public class QueryNullStrategyIT {

  private static CcmRule ccm = CcmRule.getInstance();
  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();
  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static TestMapper mapper;

  @BeforeClass
  public static void createSchema() {
    CqlSession session = sessionRule.session();

    session.execute(
        SimpleStatement.builder("CREATE TABLE foo(k int PRIMARY KEY, v int)")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    mapper = new QueryNullStrategyIT_TestMapperBuilder(session).build();
  }

  @Test
  public void should_respect_strategy_inheritance_rules() {
    if (sessionRule.session().getContext().getProtocolVersion() == DefaultProtocolVersion.V3) {
      throw new AssumptionViolatedException(
          "Test not valid for Protocol V3 as does not support UNSET");
    }

    DaoWithNoStrategy daoWithNoStrategy = mapper.daoWithNoStrategy();
    DaoWithDoNotSet daoWithDoNotSet = mapper.daoWithDoNotSet();
    DaoWithSetToNull daoWithSetToNull = mapper.daoWithSetToNull();

    assertStrategy(daoWithNoStrategy::insertWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::insertWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::insertWithSetToNull, SET_TO_NULL);

    assertStrategy(daoWithDoNotSet::insertWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::insertWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::insertWithSetToNull, SET_TO_NULL);

    assertStrategy(daoWithSetToNull::insertWithNoStrategy, SET_TO_NULL);
    assertStrategy(daoWithSetToNull::insertWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithSetToNull::insertWithSetToNull, SET_TO_NULL);
  }

  private void assertStrategy(
      BiConsumer<Integer, Integer> daoMethod, NullSavingStrategy expectedStrategy) {
    CqlSession session = sessionRule.session();
    session.execute("INSERT INTO foo (k, v) VALUES (1, 1)");
    daoMethod.accept(1, null);
    Row row = session.execute("SELECT v FROM foo WHERE k = 1").one();
    switch (expectedStrategy) {
      case DO_NOT_SET:
        assertThat(row.getInt("v")).isEqualTo(1);
        break;
      case SET_TO_NULL:
        assertThat(row.isNull("v")).isTrue();
        break;
      default:
        throw new AssertionError("unhandled strategy " + expectedStrategy);
    }
  }

  @Dao
  public interface DaoWithNoStrategy {
    @Query("INSERT INTO foo (k, v) values (:k, :v)")
    void insertWithNoStrategy(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = SET_TO_NULL)
    void insertWithSetToNull(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = DO_NOT_SET)
    void insertWithDoNotSet(Integer k, Integer v);
  }

  @Dao
  @DefaultNullSavingStrategy(SET_TO_NULL)
  public interface DaoWithSetToNull {
    @Query("INSERT INTO foo (k, v) values (:k, :v)")
    void insertWithNoStrategy(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = SET_TO_NULL)
    void insertWithSetToNull(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = DO_NOT_SET)
    void insertWithDoNotSet(Integer k, Integer v);
  }

  @Dao
  @DefaultNullSavingStrategy(DO_NOT_SET)
  public interface DaoWithDoNotSet {
    @Query("INSERT INTO foo (k, v) values (:k, :v)")
    void insertWithNoStrategy(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = SET_TO_NULL)
    void insertWithSetToNull(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = DO_NOT_SET)
    void insertWithDoNotSet(Integer k, Integer v);
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    DaoWithNoStrategy daoWithNoStrategy();

    @DaoFactory
    DaoWithSetToNull daoWithSetToNull();

    @DaoFactory
    DaoWithDoNotSet daoWithDoNotSet();
  }
}
