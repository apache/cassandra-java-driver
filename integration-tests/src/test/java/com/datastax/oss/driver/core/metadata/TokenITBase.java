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
package com.datastax.oss.driver.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;

public abstract class TokenITBase {

  protected static final CqlIdentifier KS1 = SessionUtils.uniqueKeyspaceId();
  protected static final CqlIdentifier KS2 = SessionUtils.uniqueKeyspaceId();

  // Must be called in a @BeforeClass method in each subclass (unfortunately we can't do this
  // automatically because it requires the session, which is not available from a static context in
  // this class).
  protected static void createSchema(CqlSession session) {
    for (String statement :
        ImmutableList.of(
            String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                KS1.asCql(false)),
            String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}",
                KS2.asCql(false)),

            // Shouldn't really do that, but it makes the rest of the tests a bit prettier.
            String.format("USE %s", KS1.asCql(false)),
            "CREATE TABLE foo(i int primary key)",
            "INSERT INTO  foo (i) VALUES (1)",
            "INSERT INTO  foo (i) VALUES (2)",
            "INSERT INTO  foo (i) VALUES (3)")) {
      session.execute(statement);
    }
  }

  private final String expectedPartitionerName;
  private final Class<? extends Token> expectedTokenType;
  private final boolean useVnodes;
  private final int tokensPerNode;

  protected TokenITBase(
      String expectedPartitionerName, Class<? extends Token> expectedTokenType, boolean useVnodes) {
    this.expectedPartitionerName = expectedPartitionerName;
    this.expectedTokenType = expectedTokenType;
    this.useVnodes = useVnodes;
    this.tokensPerNode = useVnodes ? 256 : 1;
  }

  protected abstract CqlSession session();

  /**
   * Validates that the token metadata is consistent with server-side range queries. That is,
   * querying the data in a range does return a PK that the driver thinks is in that range.
   *
   * @test_category metadata:token
   * @expected_result token ranges are exposed and usable.
   * @jira_ticket JAVA-312
   * @since 2.0.10, 2.1.5
   */
  @Test
  public void should_be_consistent_with_range_queries() {
    TokenMap tokenMap = getTokenMap();

    // Find the replica for a given partition key of ks1.foo.
    int key = 1;
    ProtocolVersion protocolVersion = session().getContext().getProtocolVersion();
    ByteBuffer serializedKey = TypeCodecs.INT.encodePrimitive(key, protocolVersion);
    assertThat(serializedKey).isNotNull();
    Set<Node> replicas = tokenMap.getReplicas(KS1, serializedKey);
    assertThat(replicas).hasSize(1);
    Node replica = replicas.iterator().next();

    // Iterate the cluster's token ranges. For each one, use a range query to get all the keys of
    // ks1.foo that are in this range.
    PreparedStatement rangeStatement =
        session().prepare("SELECT i FROM foo WHERE token(i) > ? and token(i) <= ?");

    TokenRange foundRange = null;
    for (TokenRange range : tokenMap.getTokenRanges()) {
      List<Row> rows = rangeQuery(rangeStatement, range);
      for (Row row : rows) {
        if (row.getInt("i") == key) {
          // We should find our initial key exactly once
          assertThat(foundRange)
              .describedAs("found the same key in two ranges: " + foundRange + " and " + range)
              .isNull();
          foundRange = range;

          // That range should be managed by the replica
          assertThat(tokenMap.getReplicas(KS1, range)).contains(replica);
        }
      }
    }
    assertThat(foundRange).isNotNull();
  }

  private List<Row> rangeQuery(PreparedStatement rangeStatement, TokenRange range) {
    List<Row> rows = Lists.newArrayList();
    for (TokenRange subRange : range.unwrap()) {
      Statement<?> statement = rangeStatement.bind(subRange.getStart(), subRange.getEnd());
      session().execute(statement).forEach(rows::add);
    }
    return rows;
  }

  /**
   * Validates that a {@link Token} can be retrieved and parsed by executing 'select token(name)'
   * and then used to find data matching that token.
   *
   * <p>This test does the following: retrieve the token for the key with value '1', get it by
   * index, and ensure if is of the expected token type; select data by token with a BoundStatement;
   * select data by token using setToken by index.
   *
   * @test_category token
   * @expected_result tokens are selectable, properly parsed, and usable as input.
   * @jira_ticket JAVA-312
   * @since 2.0.10, 2.1.5
   */
  @Test
  public void should_get_token_from_row_and_set_token_in_query() {
    ResultSet rs = session().execute("SELECT token(i) FROM foo WHERE i = 1");
    Row row = rs.one();
    assertThat(row).isNotNull();

    // Get by index:
    Token token = row.getToken(0);
    assertThat(token).isNotNull().isInstanceOf(expectedTokenType);

    // Get by name: the generated column name depends on the Cassandra version.
    String tokenColumnName =
        rs.getColumnDefinitions().contains("token(i)") ? "token(i)" : "system.token(i)";
    assertThat(row.getToken(tokenColumnName)).isEqualTo(token);

    PreparedStatement pst = session().prepare("SELECT * FROM foo WHERE token(i) = ?");
    // Bind with bind(...)
    row = session().execute(pst.bind(token)).iterator().next();
    assertThat(row.getInt(0)).isEqualTo(1);

    // Bind with setToken by index
    row = session().execute(pst.bind().setToken(0, token)).one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(1);

    // Bind with setToken by name
    row = session().execute(pst.bind().setToken("partition key token", token)).one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  /**
   * Validates that a {@link Token} can be retrieved and parsed by using bind variables and
   * aliasing.
   *
   * <p>This test does the following: retrieve the token by alias for the key '1', and ensure it
   * matches the token by index; select data by token using setToken by name.
   */
  @Test
  public void should_get_token_from_row_and_set_token_in_query_with_binding_and_aliasing() {
    Row row = session().execute("SELECT token(i) AS t FROM foo WHERE i = 1").one();
    assertThat(row).isNotNull();
    Token token = row.getToken("t");
    assertThat(token).isNotNull().isInstanceOf(expectedTokenType);

    PreparedStatement pst = session().prepare("SELECT * FROM foo WHERE token(i) = :myToken");
    row = session().execute(pst.bind().setToken("myToken", token)).one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(1);

    row =
        session()
            .execute(SimpleStatement.newInstance("SELECT * FROM foo WHERE token(i) = ?", token))
            .one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  /**
   * Ensures that an exception is raised when attempting to retrieve a token from a column that
   * doesn't match the CQL type of any token type.
   *
   * @test_category token
   * @expected_result an exception is raised.
   * @jira_ticket JAVA-312
   * @since 2.0.10, 2.1.5
   */
  @Test(expected = IllegalArgumentException.class)
  public void should_raise_exception_when_getting_token_on_non_token_column() {
    Row row = session().execute("SELECT i FROM foo WHERE i = 1").one();
    assertThat(row).isNotNull();
    row.getToken(0);
  }

  /**
   * Ensures that token ranges are exposed per node, the ranges are complete, the entire ring is
   * represented, and that ranges do not overlap.
   *
   * @test_category metadata:token
   * @expected_result The entire token range is represented collectively and the ranges do not
   *     overlap.
   * @jira_ticket JAVA-312
   * @since 2.0.10, 2.1.5
   */
  @Test
  public void should_expose_consistent_ranges() {
    checkRanges(session());
    checkRanges(session(), KS1, 1);
    checkRanges(session(), KS2, 2);
  }

  private void checkRanges(Session session) {
    assertThat(session.getMetadata().getTokenMap()).isPresent();
    TokenMap tokenMap = session.getMetadata().getTokenMap().get();
    checkRanges(tokenMap.getTokenRanges());
  }

  private void checkRanges(Session session, CqlIdentifier keyspace, int replicationFactor) {
    assertThat(session.getMetadata().getTokenMap()).isPresent();
    TokenMap tokenMap = session.getMetadata().getTokenMap().get();
    List<TokenRange> allRangesWithDuplicates = Lists.newArrayList();

    // Get each host's ranges, the count should match the replication factor
    for (Node node : session.getMetadata().getNodes().values()) {
      Set<TokenRange> hostRanges = tokenMap.getTokenRanges(keyspace, node);
      // Special case: When using vnodes the tokens are not evenly assigned to each replica.
      if (!useVnodes) {
        assertThat(hostRanges)
            .as(
                "Node %s: expected %d ranges, got %d",
                node, replicationFactor * tokensPerNode, hostRanges.size())
            .hasSize(replicationFactor * tokensPerNode);
      }
      allRangesWithDuplicates.addAll(hostRanges);
    }

    // Special case check for vnodes to ensure that total number of replicated ranges is correct.
    assertThat(allRangesWithDuplicates)
        .as(
            "Expected %d total replicated ranges with duplicates, got %d",
            3 * replicationFactor * tokensPerNode, allRangesWithDuplicates.size())
        .hasSize(3 * replicationFactor * tokensPerNode);

    // Once we ignore duplicates, the number of ranges should match the number of nodes.
    Set<TokenRange> allRanges = new TreeSet<>(allRangesWithDuplicates);
    assertThat(allRanges)
        .as("Expected %d total replicated ranges, got %d", 3 * tokensPerNode, allRanges.size())
        .hasSize(3 * tokensPerNode);

    // And the ranges should cover the whole ring and no ranges intersect.
    checkRanges(allRanges);
  }

  // Ensures that no ranges intersect and that they cover the entire ring.
  private void checkRanges(Collection<TokenRange> ranges) {
    // Ensure no ranges intersect.
    TokenRange[] rangesArray = ranges.toArray(new TokenRange[0]);
    for (int i = 0; i < rangesArray.length; i++) {
      TokenRange rangeI = rangesArray[i];
      for (int j = i + 1; j < rangesArray.length; j++) {
        TokenRange rangeJ = rangesArray[j];
        assertThat(rangeI.intersects(rangeJ))
            .as("Range " + rangeI + " intersects with " + rangeJ)
            .isFalse();
      }
    }

    // Ensure the defined ranges cover the entire ring.
    Iterator<TokenRange> it = ranges.iterator();
    TokenRange mergedRange = it.next();
    while (it.hasNext()) {
      TokenRange next = it.next();
      mergedRange = mergedRange.mergeWith(next);
    }
    boolean isFullRing =
        mergedRange.getStart().equals(mergedRange.getEnd()) && !mergedRange.isEmpty();
    assertThat(isFullRing).as("Ring is not fully defined for cluster.").isTrue();
  }

  /**
   * Ensures that for there is at most one wrapped range in the ring, and check that unwrapping it
   * produces two ranges.
   *
   * @test_category metadata:token
   * @expected_result there is at most one wrapped range.
   * @jira_ticket JAVA-312
   * @since 2.0.10, 2.1.5
   */
  @Test
  public void should_have_only_one_wrapped_range() {
    TokenMap tokenMap = getTokenMap();
    TokenRange wrappedRange = null;
    for (TokenRange range : tokenMap.getTokenRanges()) {
      if (range.isWrappedAround()) {
        assertThat(wrappedRange)
            .as(
                "Found a wrapped around TokenRange (%s) when one already exists (%s).",
                range, wrappedRange)
            .isNull();
        wrappedRange = range;

        assertThat(wrappedRange.unwrap()).hasSize(2);
      }
    }
  }

  @Test
  public void should_create_tokens_and_ranges() {
    TokenMap tokenMap = getTokenMap();

    // Pick a random range
    TokenRange range = tokenMap.getTokenRanges().iterator().next();

    Token start = tokenMap.parse(tokenMap.format(range.getStart()));
    Token end = tokenMap.parse(tokenMap.format(range.getEnd()));

    assertThat(tokenMap.newTokenRange(start, end)).isEqualTo(range);
  }

  @Test
  public void should_create_token_from_partition_key() {
    TokenMap tokenMap = getTokenMap();

    Row row = session().execute("SELECT token(i) FROM foo WHERE i = 1").one();
    assertThat(row).isNotNull();
    Token expected = row.getToken(0);

    ProtocolVersion protocolVersion = session().getContext().getProtocolVersion();
    assertThat(tokenMap.newToken(TypeCodecs.INT.encodePrimitive(1, protocolVersion)))
        .isEqualTo(expected);
  }

  private TokenMap getTokenMap() {
    return session()
        .getMetadata()
        .getTokenMap()
        .map(
            tokenMap -> {
              assertThat(tokenMap.getPartitionerName()).isEqualTo(expectedPartitionerName);
              return tokenMap;
            })
        .orElseThrow(() -> new AssertionError("Expected token map to be present"));
  }
}
