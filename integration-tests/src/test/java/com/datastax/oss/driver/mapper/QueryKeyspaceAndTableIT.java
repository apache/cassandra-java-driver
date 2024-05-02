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
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** Covers the keyspace and table placeholders in {@link Query} methods. */
@Category(ParallelizableTests.class)
public class QueryKeyspaceAndTableIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final CqlIdentifier FOO_TABLE_ID = CqlIdentifier.fromCql("foo");
  private static final CqlIdentifier OTHER_KEYSPACE =
      CqlIdentifier.fromCql(QueryKeyspaceAndTableIT.class.getSimpleName() + "_alt");

  private static TestMapper mapper;

  @BeforeClass
  public static void createSchema() {
    CqlSession session = SESSION_RULE.session();

    for (String query :
        ImmutableList.of(
            "CREATE TABLE foo(k int PRIMARY KEY)",
            String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                OTHER_KEYSPACE.asCql(false)),
            String.format("CREATE TABLE %s.foo(k int PRIMARY KEY)", OTHER_KEYSPACE.asCql(false)))) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    session.execute("INSERT INTO foo (k) VALUES (1)");
    session.execute(
        String.format("INSERT INTO %s.foo (k) VALUES (1)", OTHER_KEYSPACE.asCql(false)));
    session.execute(
        String.format("INSERT INTO %s.foo (k) VALUES (2)", OTHER_KEYSPACE.asCql(false)));

    mapper = new QueryKeyspaceAndTableIT_TestMapperBuilder(session).build();
  }

  @Test
  public void should_substitute_keyspaceId_and_tableId() {
    DaoWithKeyspaceAndTableId dao =
        mapper.daoWithKeyspaceAndTableId(SESSION_RULE.keyspace(), FOO_TABLE_ID);
    assertThat(dao.count()).isEqualTo(1);
  }

  @Test
  public void should_fail_to_substitute_keyspaceId_if_dao_has_no_keyspace() {
    Throwable t = catchThrowable(() -> mapper.daoWithKeyspaceAndTableId(null, FOO_TABLE_ID));
    assertThat(t)
        .isInstanceOf(MapperException.class)
        .hasMessage(
            "Cannot substitute ${keyspaceId} in query "
                + "'SELECT count(*) FROM ${keyspaceId}.${tableId}': "
                + "the DAO wasn't built with a keyspace");
  }

  @Test
  public void should_fail_to_substitute_tableId_if_dao_has_no_table() {
    Throwable t =
        catchThrowable(() -> mapper.daoWithKeyspaceAndTableId(SESSION_RULE.keyspace(), null));
    assertThat(t)
        .isInstanceOf(MapperException.class)
        .hasMessage(
            "Cannot substitute ${tableId} in query "
                + "'SELECT count(*) FROM ${keyspaceId}.${tableId}': "
                + "the DAO wasn't built with a table");
  }

  @Test
  public void should_use_keyspace_in_qualifiedTableId_when_dao_has_keyspace() {
    DaoWithQualifiedTableId dao = mapper.daoWithQualifiedTableId(OTHER_KEYSPACE, FOO_TABLE_ID);
    assertThat(dao.count()).isEqualTo(2);
  }

  @Test
  public void should_not_use_keyspace_in_qualifiedTableId_when_dao_has_no_keyspace() {
    DaoWithQualifiedTableId dao = mapper.daoWithQualifiedTableId(null, FOO_TABLE_ID);
    assertThat(dao.count()).isEqualTo(1);
  }

  @Test
  public void should_fail_to_substitute_qualifiedTableId_if_dao_has_no_table() {
    Throwable t =
        catchThrowable(() -> mapper.daoWithQualifiedTableId(SESSION_RULE.keyspace(), null));
    assertThat(t)
        .isInstanceOf(MapperException.class)
        .hasMessage(
            "Cannot substitute ${qualifiedTableId} in query "
                + "'SELECT count(*) FROM ${qualifiedTableId}': "
                + "the DAO wasn't built with a table");
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DaoWithKeyspaceAndTableId {
    @Query("SELECT count(*) FROM ${keyspaceId}.${tableId}")
    long count();
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DaoWithQualifiedTableId {
    @Query("SELECT count(*) FROM ${qualifiedTableId}")
    long count();
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    DaoWithKeyspaceAndTableId daoWithKeyspaceAndTableId(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    DaoWithQualifiedTableId daoWithQualifiedTableId(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);
  }
}
