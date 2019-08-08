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

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.SET_TO_NULL;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * For JAVA-2367: ensure that PK column names are properly handled in the WHERE clause of a
 * generated UPDATE query.
 */
@Category(ParallelizableTests.class)
public class UpdateNamingIT {
  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        SimpleStatement.builder("CREATE TABLE foo(mykey int PRIMARY KEY, value int)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    TestMapper mapper =
        TestMapper.builder(session).withDefaultKeyspace(SESSION_RULE.keyspace()).build();
    dao = mapper.dao();
  }

  @Test
  public void should_update_with_case_insensitive_pk_name() {
    dao.update(new Foo(1, 1));
    Foo foo = dao.get(1);
    assertThat(foo.getValue()).isEqualTo(1);
  }

  @Mapper
  public interface TestMapper {

    @DaoFactory
    TestDao dao();

    static MapperBuilder<TestMapper> builder(CqlSession session) {
      return new UpdateNamingIT_TestMapperBuilder(session);
    }
  }

  @Dao
  @DefaultNullSavingStrategy(SET_TO_NULL)
  public interface TestDao {
    @Select
    Foo get(int key);

    @Update
    void update(Foo template);
  }

  @Entity
  @NamingStrategy(convention = NamingConvention.CASE_INSENSITIVE)
  public static class Foo {
    @PartitionKey private int myKey;
    private int value;

    public Foo() {}

    public Foo(int myKey, int value) {
      this.myKey = myKey;
      this.value = value;
    }

    public int getMyKey() {
      return myKey;
    }

    public void setMyKey(int myKey) {
      this.myKey = myKey;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }
}
