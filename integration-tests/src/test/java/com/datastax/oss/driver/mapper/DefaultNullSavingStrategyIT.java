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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Covers null saving strategy interaction between DAO method annotations and {@link
 * DefaultNullSavingStrategy} annotation.
 */
@Category(ParallelizableTests.class)
@CassandraRequirement(min = "2.2", description = "support for unset values")
public class DefaultNullSavingStrategyIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestMapper mapper;
  private static PreparedStatement prepared;

  @BeforeClass
  public static void createSchema() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.builder("CREATE TABLE foo(k int PRIMARY KEY, v int)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    mapper = new DefaultNullSavingStrategyIT_TestMapperBuilder(session).build();
    prepared = SESSION_RULE.session().prepare("INSERT INTO foo (k, v) values (:k, :v)");
  }

  @Test
  public void should_respect_strategy_inheritance_rules() {
    DaoWithNoStrategy daoWithNoStrategy = mapper.daoWithNoStrategy();
    DaoWithDoNotSet daoWithDoNotSet = mapper.daoWithDoNotSet();
    DaoWithSetToNull daoWithSetToNull = mapper.daoWithSetToNull();

    assertStrategy(daoWithNoStrategy::queryWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::queryWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::queryWithSetToNull, SET_TO_NULL);
    assertStrategy(daoWithNoStrategy::insertWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::insertWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::insertWithSetToNull, SET_TO_NULL);
    assertStrategy(daoWithNoStrategy::updateWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::updateWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithNoStrategy::updateWithSetToNull, SET_TO_NULL);
    assertSetEntityStrategy(daoWithNoStrategy::setWithNoStrategy, DO_NOT_SET);
    assertSetEntityStrategy(daoWithNoStrategy::setWithDoNotSet, DO_NOT_SET);
    assertSetEntityStrategy(daoWithNoStrategy::setWithSetToNull, SET_TO_NULL);

    assertStrategy(daoWithDoNotSet::queryWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::queryWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::queryWithSetToNull, SET_TO_NULL);
    assertStrategy(daoWithDoNotSet::insertWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::insertWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::insertWithSetToNull, SET_TO_NULL);
    assertStrategy(daoWithDoNotSet::updateWithNoStrategy, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::updateWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithDoNotSet::updateWithSetToNull, SET_TO_NULL);
    assertSetEntityStrategy(daoWithDoNotSet::setWithNoStrategy, DO_NOT_SET);
    assertSetEntityStrategy(daoWithDoNotSet::setWithDoNotSet, DO_NOT_SET);
    assertSetEntityStrategy(daoWithDoNotSet::setWithSetToNull, SET_TO_NULL);

    assertStrategy(daoWithSetToNull::queryWithNoStrategy, SET_TO_NULL);
    assertStrategy(daoWithSetToNull::queryWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithSetToNull::queryWithSetToNull, SET_TO_NULL);
    assertStrategy(daoWithSetToNull::insertWithNoStrategy, SET_TO_NULL);
    assertStrategy(daoWithSetToNull::insertWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithSetToNull::insertWithSetToNull, SET_TO_NULL);
    assertStrategy(daoWithSetToNull::updateWithNoStrategy, SET_TO_NULL);
    assertStrategy(daoWithSetToNull::updateWithDoNotSet, DO_NOT_SET);
    assertStrategy(daoWithSetToNull::updateWithSetToNull, SET_TO_NULL);
    assertSetEntityStrategy(daoWithSetToNull::setWithNoStrategy, SET_TO_NULL);
    assertSetEntityStrategy(daoWithSetToNull::setWithDoNotSet, DO_NOT_SET);
    assertSetEntityStrategy(daoWithSetToNull::setWithSetToNull, SET_TO_NULL);
  }

  private void assertStrategy(
      BiConsumer<Integer, Integer> daoMethod, NullSavingStrategy expectedStrategy) {
    reset();
    daoMethod.accept(1, null);
    validateData(expectedStrategy);
  }

  private void assertStrategy(Consumer<Foo> daoMethod, NullSavingStrategy expectedStrategy) {
    reset();
    Foo foo = new Foo(1, null);
    daoMethod.accept(foo);
    validateData(expectedStrategy);
  }

  private void assertSetEntityStrategy(
      BiConsumer<BoundStatementBuilder, Foo> daoMethod, NullSavingStrategy expectedStrategy) {
    reset();
    Foo foo = new Foo(1, null);
    BoundStatementBuilder builder = prepared.boundStatementBuilder();
    daoMethod.accept(builder, foo);
    SESSION_RULE.session().execute(builder.build());
    validateData(expectedStrategy);
  }

  private void reset() {
    CqlSession session = SESSION_RULE.session();
    session.execute("INSERT INTO foo (k, v) VALUES (1, 1)");
  }

  private void validateData(NullSavingStrategy expectedStrategy) {
    CqlSession session = SESSION_RULE.session();
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
    void queryWithNoStrategy(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = SET_TO_NULL)
    void queryWithSetToNull(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = DO_NOT_SET)
    void queryWithDoNotSet(Integer k, Integer v);

    @Insert
    void insertWithNoStrategy(Foo foo);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void insertWithSetToNull(Foo foo);

    @Insert(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void insertWithDoNotSet(Foo foo);

    @Update
    void updateWithNoStrategy(Foo foo);

    @Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void updateWithSetToNull(Foo foo);

    @Update(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void updateWithDoNotSet(Foo foo);

    @SetEntity
    void setWithNoStrategy(BoundStatementBuilder builder, Foo foo);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void setWithSetToNull(BoundStatementBuilder builder, Foo foo);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void setWithDoNotSet(BoundStatementBuilder builder, Foo foo);
  }

  @Dao
  @DefaultNullSavingStrategy(SET_TO_NULL)
  public interface DaoWithSetToNull {
    @Query("INSERT INTO foo (k, v) values (:k, :v)")
    void queryWithNoStrategy(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = SET_TO_NULL)
    void queryWithSetToNull(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = DO_NOT_SET)
    void queryWithDoNotSet(Integer k, Integer v);

    @Insert
    void insertWithNoStrategy(Foo foo);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void insertWithSetToNull(Foo foo);

    @Insert(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void insertWithDoNotSet(Foo foo);

    @Update
    void updateWithNoStrategy(Foo foo);

    @Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void updateWithSetToNull(Foo foo);

    @Update(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void updateWithDoNotSet(Foo foo);

    @SetEntity
    void setWithNoStrategy(BoundStatementBuilder builder, Foo foo);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void setWithSetToNull(BoundStatementBuilder builder, Foo foo);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void setWithDoNotSet(BoundStatementBuilder builder, Foo foo);
  }

  @Dao
  @DefaultNullSavingStrategy(DO_NOT_SET)
  public interface DaoWithDoNotSet {
    @Query("INSERT INTO foo (k, v) values (:k, :v)")
    void queryWithNoStrategy(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = SET_TO_NULL)
    void queryWithSetToNull(Integer k, Integer v);

    @Query(value = "INSERT INTO foo (k, v) values (:k, :v)", nullSavingStrategy = DO_NOT_SET)
    void queryWithDoNotSet(Integer k, Integer v);

    @Insert
    void insertWithNoStrategy(Foo foo);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void insertWithSetToNull(Foo foo);

    @Insert(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void insertWithDoNotSet(Foo foo);

    @Update
    void updateWithNoStrategy(Foo foo);

    @Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void updateWithSetToNull(Foo foo);

    @Update(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void updateWithDoNotSet(Foo foo);

    @SetEntity
    void setWithNoStrategy(BoundStatementBuilder builder, Foo foo);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void setWithSetToNull(BoundStatementBuilder builder, Foo foo);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void setWithDoNotSet(BoundStatementBuilder builder, Foo foo);
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

  @Entity
  public static class Foo {
    @PartitionKey private int k;
    private Integer v;

    public Foo() {
      this.k = 0;
      this.v = null;
    }

    public Foo(int k, Integer v) {
      this.k = k;
      this.v = v;
    }

    public int getK() {
      return k;
    }

    public void setK(int k) {
      this.k = k;
    }

    public Integer getV() {
      return v;
    }

    public void setV(Integer v) {
      this.v = v;
    }
  }
}
