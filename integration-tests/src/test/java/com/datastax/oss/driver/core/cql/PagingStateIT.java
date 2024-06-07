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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.ccm.SchemaChangeSynchronizer;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.function.UnaryOperator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class PagingStateIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule public static TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Before
  public void setupSchema() {
    CqlSession session = SESSION_RULE.session();
    SchemaChangeSynchronizer.withLock(
        () -> {
          session.execute(
              SimpleStatement.builder(
                      "CREATE TABLE IF NOT EXISTS foo (k int, cc int, v int, PRIMARY KEY(k, cc))")
                  .setExecutionProfile(SESSION_RULE.slowProfile())
                  .build());
        });
    for (int i = 0; i < 20; i++) {
      session.execute(
          SimpleStatement.newInstance("INSERT INTO foo (k, cc, v) VALUES (1, ?, ?)", i, i));
    }
  }

  @Test
  public void should_extract_and_reuse() {
    should_extract_and_reuse(UnaryOperator.identity());
  }

  @Test
  public void should_convert_to_bytes() {
    should_extract_and_reuse(pagingState -> PagingState.fromBytes(pagingState.toBytes()));
  }

  @Test
  public void should_convert_to_string() {
    should_extract_and_reuse(pagingState -> PagingState.fromString(pagingState.toString()));
  }

  private void should_extract_and_reuse(UnaryOperator<PagingState> transformation) {
    CqlSession session = SESSION_RULE.session();

    BoundStatement boundStatement =
        session
            .prepare(SimpleStatement.newInstance("SELECT * FROM foo WHERE k = ?").setPageSize(15))
            .bind(1);

    ResultSet resultSet = session.execute(boundStatement);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(15);
    assertThat(resultSet.isFullyFetched()).isFalse();

    PagingState pagingState =
        transformation.apply(resultSet.getExecutionInfo().getSafePagingState());

    assertThat(pagingState.matches(boundStatement)).isTrue();
    resultSet = session.execute(boundStatement.setPagingState(pagingState));
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(5);
    assertThat(resultSet.isFullyFetched()).isTrue();
  }

  @Test
  public void should_inject_in_simple_statement_with_custom_codecs() {
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addTypeCodecs(new IntWrapperCodec())
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withKeyspace(SESSION_RULE.keyspace())
                .build()) {

      SimpleStatement statement =
          SimpleStatement.newInstance("SELECT * FROM foo WHERE k = ?", new IntWrapper(1))
              .setPageSize(15);

      ResultSet resultSet = session.execute(statement);
      assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(15);
      assertThat(resultSet.isFullyFetched()).isFalse();

      PagingState pagingState = resultSet.getExecutionInfo().getSafePagingState();

      // This is the case where we need the session: simple statements are not attached, so
      // setPagingState() cannot find the custom codec.
      try {
        @SuppressWarnings("unused")
        SimpleStatement ignored = statement.setPagingState(pagingState);
        fail("Expected a CodecNotFoundException");
      } catch (CodecNotFoundException e) {
        // expected
      }

      resultSet = session.execute(statement.setPagingState(pagingState, session));
      assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(5);
      assertThat(resultSet.isFullyFetched()).isTrue();
    }
  }

  @Test
  public void should_fail_if_query_does_not_match() {
    should_fail("SELECT * FROM foo WHERE k = ?", 1, "SELECT v FROM FOO WHERE k = ?", 1);
  }

  @Test
  public void should_fail_if_values_do_not_match() {
    should_fail("SELECT * FROM foo WHERE k = ?", 1, "SELECT * FROM foo WHERE k = ?", 2);
  }

  private void should_fail(String query1, int value1, String query2, int value2) {
    CqlSession session = SESSION_RULE.session();

    BoundStatement boundStatement1 =
        session.prepare(SimpleStatement.newInstance(query1).setPageSize(15)).bind(value1);

    ResultSet resultSet = session.execute(boundStatement1);
    PagingState pagingState = resultSet.getExecutionInfo().getSafePagingState();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    Throwable t =
        catchThrowable(
            () ->
                session
                    .prepare(SimpleStatement.newInstance(query2).setPageSize(15))
                    .bind(value2)
                    .setPagingState(pagingState));

    assertThat(t).isInstanceOf(IllegalArgumentException.class);
  }

  static class IntWrapper {
    final int value;

    public IntWrapper(int value) {
      this.value = value;
    }
  }

  static class IntWrapperCodec extends MappingCodec<Integer, IntWrapper> {

    protected IntWrapperCodec() {
      super(new IntCodec(), GenericType.of(IntWrapper.class));
    }

    @Nullable
    @Override
    protected IntWrapper innerToOuter(@Nullable Integer value) {
      return value == null ? null : new IntWrapper(value);
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable IntWrapper wrapper) {
      return wrapper == null ? null : wrapper.value;
    }
  }
}
