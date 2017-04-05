/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * This class uses subclasses for each type of partitioner.
 * <p/>
 * There's normally a way to parametrize a TestNG class with @Factory and @DataProvider,
 * but it doesn't seem to work with multiple methods.
 */
@CCMConfig(numberOfNodes = 3, createKeyspace = false)
public abstract class TokenIntegrationTest extends CCMTestsSupport {

    private final DataType expectedTokenType;
    private final int numTokens;
    private final boolean useVnodes;
    private String ks1;
    private String ks2;

    public TokenIntegrationTest(DataType expectedTokenType, boolean useVnodes) {
        this.expectedTokenType = expectedTokenType;
        this.numTokens = useVnodes ? 256 : 1;
        this.useVnodes = useVnodes;
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        // Only connect to node 1, which makes it easier to query system tables in should_expose_tokens_per_host()
        LoadBalancingPolicy lbp = new WhiteListPolicy(new RoundRobinPolicy(),
                Collections.singleton(ccm().addressOfNode(1)));
        return Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withLoadBalancingPolicy(lbp);
    }

    @Override
    public void onTestContextInitialized() {
        ks1 = TestUtils.generateIdentifier("ks_");
        ks2 = TestUtils.generateIdentifier("ks_");
        execute(
                String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ks1),
                String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}", ks2),
                String.format("USE %s", ks1),
                "CREATE TABLE foo(i int primary key)",
                "INSERT INTO foo (i) VALUES (1)",
                "INSERT INTO foo (i) VALUES (2)",
                "INSERT INTO foo (i) VALUES (3)"
        );
    }

    /**
     * <p>
     * Validates that {@link TokenRange}s are exposed via a {@link Cluster}'s {@link Metadata} and they
     * can be used to query data.
     * </p>
     *
     * @test_category metadata:token
     * @expected_result token ranges are exposed and usable.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short")
    public void should_expose_token_ranges() throws Exception {
        Metadata metadata = cluster().getMetadata();

        // Find the replica for a given partition key
        int testKey = 1;
        Set<Host> replicas = metadata.getReplicas(ks1, TypeCodec.cint().serialize(testKey, cluster().getConfiguration().getProtocolOptions().getProtocolVersion()));
        assertThat(replicas).hasSize(1);
        Host replica = replicas.iterator().next();

        // Iterate the cluster's token ranges. For each one, use a range query to ask Cassandra which partition keys
        // are in this range.

        PreparedStatement rangeStmt = session().prepare("SELECT i FROM foo WHERE token(i) > ? and token(i) <= ?");

        TokenRange foundRange = null;
        for (TokenRange range : metadata.getTokenRanges()) {
            List<Row> rows = rangeQuery(rangeStmt, range);
            for (Row row : rows) {
                if (row.getInt("i") == testKey) {
                    // We should find our test key exactly once
                    assertThat(foundRange)
                            .describedAs("found the same key in two ranges: " + foundRange + " and " + range)
                            .isNull();
                    foundRange = range;
                    // That range should be managed by the replica
                    assertThat(metadata.getReplicas(ks1, range)).contains(replica);
                }
            }
        }
        assertThat(foundRange).isNotNull();
    }

    private List<Row> rangeQuery(PreparedStatement rangeStmt, TokenRange range) {
        List<Row> rows = Lists.newArrayList();
        for (TokenRange subRange : range.unwrap()) {
            Statement statement = rangeStmt.bind(subRange.getStart(), subRange.getEnd());
            rows.addAll(session().execute(statement).all());
        }
        return rows;
    }

    /**
     * <p>
     * Validates that a {@link Token} can be retrieved and parsed by executing 'select token(name)' and
     * then used to find data matching that token.
     * </p>
     * <p/>
     * <p/>
     * This test does the following:
     * <p/>
     * <ol>
     * <li>Retrieve the token for the key with value '1', get it by index, and ensure if is of the expected token type.</li>
     * <li>Retrieve the token for the partition key with getPartitionKeyToken</li>
     * <li>Select data by token with a BoundStatement.</li>
     * <li>Select data by token using setToken by index.</li>
     * <li>Select data by token with setPartitionKeyToken.</li>
     * </ol>
     *
     * @test_category token
     * @expected_result tokens are selectable, properly parsed, and usable as input.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short")
    public void should_get_token_from_row_and_set_token_in_query() {
        // get by index:
        Row row = session().execute("SELECT token(i) FROM foo WHERE i = 1").one();
        Token token = row.getToken(0);
        assertThat(token.getType()).isEqualTo(expectedTokenType);

        assertThat(
                row.getPartitionKeyToken()
        ).isEqualTo(token);

        PreparedStatement pst = session().prepare("SELECT * FROM foo WHERE token(i) = ?");
        row = session().execute(pst.bind(token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session().execute(pst.bind().setToken(0, token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session().execute(pst.bind().setPartitionKeyToken(token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);
    }

    /**
     * <p>
     * Validates that a {@link Token} can be retrieved and parsed by using bind variables and
     * aliasing.
     * </p>
     * <p/>
     * <p/>
     * This test does the following:
     * <p/>
     * <ol>
     * <li>Retrieve the token by alias for the key '1', and ensure it matches the token by index.</li>
     * <li>Select data by token using setToken by name.</li>
     * </ol>
     */
    @Test(groups = "short")
    @CassandraVersion("2.0")
    public void should_get_token_from_row_and_set_token_in_query_with_binding_and_aliasing() {
        Row row = session().execute("SELECT token(i) AS t FROM foo WHERE i = 1").one();
        Token token = row.getToken("t");
        assertThat(token.getType()).isEqualTo(expectedTokenType);

        PreparedStatement pst = session().prepare("SELECT * FROM foo WHERE token(i) = :myToken");
        row = session().execute(pst.bind().setToken("myToken", token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session().execute("SELECT * FROM foo WHERE token(i) = ?", token).one();
        assertThat(row.getInt(0)).isEqualTo(1);
    }

    /**
     * <p>
     * Ensures that an exception is raised when attempting to retrieve a token a non-token column.
     * </p>
     *
     * @test_category token
     * @expected_result an exception is raised.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short", expectedExceptions = InvalidTypeException.class)
    public void should_raise_exception_when_get_token_on_non_token() {
        Row row = session().execute("SELECT i FROM foo WHERE i = 1").one();
        row.getToken(0);
    }

    /**
     * <p>
     * Ensures that @{link TokenRange}s are exposed at a per host level, the ranges are complete,
     * the entire ring is represented, and that ranges do not overlap.
     * </p>
     * <p/>
     * <p>
     * Also ensures that ranges from another replica are present when a Host is a replica for
     * another node.
     * </p>
     *
     * @test_category metadata:token
     * @expected_result The entire token range is represented collectively and the ranges do not overlap.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short")
    public void should_expose_token_ranges_per_host() {
        checkRangesPerHost(ks1, 1);
        checkRangesPerHost(ks2, 2);
        assertThat(cluster()).hasValidTokenRanges();
    }

    private void checkRangesPerHost(String keyspace, int replicationFactor) {
        List<TokenRange> allRangesWithReplicas = Lists.newArrayList();

        // Get each host's ranges, the count should match the replication factor
        for (int i = 1; i <= 3; i++) {
            Host host = TestUtils.findHost(cluster(), i);
            Set<TokenRange> hostRanges = cluster().getMetadata().getTokenRanges(keyspace, host);
            // Special case: When using vnodes the tokens are not evenly assigned to each replica.
            if (!useVnodes) {
                assertThat(hostRanges).hasSize(replicationFactor * numTokens);
            }
            allRangesWithReplicas.addAll(hostRanges);
        }

        // Special case check for vnodes to ensure that total number of replicated ranges is correct.
        assertThat(allRangesWithReplicas).hasSize(3 * numTokens * replicationFactor);

        // Once we ignore duplicates, the number of ranges should match the number of nodes.
        Set<TokenRange> allRanges = new HashSet<TokenRange>(allRangesWithReplicas);
        assertThat(allRanges).hasSize(3 * numTokens);

        // And the ranges should cover the whole ring and no ranges intersect.
        assertThat(cluster()).hasValidTokenRanges(keyspace);
    }

    /**
     * <p>
     * Ensures that Tokens are exposed for each Host and that the match those in the system tables.
     * </p>
     * <p/>
     * <p>
     * Also validates that tokens are not present for multiple hosts.
     * </p>
     *
     * @test_category metadata:token
     * @expected_result Tokens are exposed by Host and match those in the system tables.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short")
    public void should_expose_tokens_per_host() {
        for (Host host : cluster().getMetadata().allHosts()) {
            assertThat(host.getTokens()).hasSize(numTokens);
            // Check against the info in the system tables, which is a bit weak since it's exactly how the metadata is
            // constructed in the first place, but there's not much else we can do.
            // Note that this relies on all queries going to node 1, which is why we use a WhiteList LBP in setup().
            boolean isControlHost = host.getSocketAddress().equals(cluster().manager.controlConnection.connectionRef.get().address);
            Row row;
            if (isControlHost) {
                row = session().execute("select tokens from system.local").one();
            } else {
                // non-control hosts are populated from system.peers and their broadcast address should be known
                assertThat(host.getBroadcastAddress()).isNotNull();
                row = session().execute("select tokens from system.peers where peer = '" + host.getBroadcastAddress().getHostAddress() + "'").one();
            }
            Set<String> tokenStrings = row.getSet("tokens", String.class);
            assertThat(tokenStrings).hasSize(numTokens);
            Iterable<Token> tokensFromSystemTable = Iterables.transform(tokenStrings, new Function<String, Token>() {
                @Override
                public Token apply(String input) {
                    return tokenFactory().fromString(input);
                }
            });

            assertThat(host.getTokens()).containsOnlyOnce(Iterables.toArray(tokensFromSystemTable, Token.class));
        }
    }

    /**
     * <p>
     * Ensures that for the {@link TokenRange}s returned by {@link Metadata#getTokenRanges()} that there exists at
     * most one {@link TokenRange} for which calling {@link TokenRange#isWrappedAround()} returns true and
     * {@link TokenRange#unwrap()} returns two {@link TokenRange}s.
     * </p>
     *
     * @test_category metadata:token
     * @expected_result Tokens are exposed by Host and match those in the system tables.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short")
    public void should_only_unwrap_one_range_for_all_ranges() {
        Set<TokenRange> ranges = cluster().getMetadata().getTokenRanges();

        assertOnlyOneWrapped(ranges);

        Iterable<TokenRange> splitRanges = Iterables.concat(Iterables.transform(ranges,
                new Function<TokenRange, Iterable<TokenRange>>() {
                    @Override
                    public Iterable<TokenRange> apply(TokenRange input) {
                        return input.splitEvenly(10);
                    }
                })
        );

        assertOnlyOneWrapped(splitRanges);
    }

    /**
     * Asserts that given the input {@link TokenRange}s that at most one of them wraps the token ring.
     *
     * @param ranges Ranges to validate against.
     */
    protected void assertOnlyOneWrapped(Iterable<TokenRange> ranges) {
        TokenRange wrappedRange = null;

        for (TokenRange range : ranges) {
            if (range.isWrappedAround()) {
                assertThat(wrappedRange)
                        .as("Found a wrapped around TokenRange (%s) when one already exists (%s).", range, wrappedRange)
                        .isNull();
                wrappedRange = range;

                assertThat(range).isWrappedAround(); // this also checks the unwrapped ranges
            } else {
                assertThat(range).isNotWrappedAround();
            }
        }
    }

    @Test(groups = "short")
    public void should_expose_token_and_range_creation_methods() {
        Metadata metadata = cluster().getMetadata();

        // Pick a random range
        TokenRange range = metadata.getTokenRanges().iterator().next();

        Token start = metadata.newToken(range.getStart().toString());
        Token end = metadata.newToken(range.getEnd().toString());

        assertThat(metadata.newTokenRange(start, end))
                .isEqualTo(range);
    }

    @Test(groups = "short")
    public void should_create_token_from_partition_key() {
        Metadata metadata = cluster().getMetadata();

        Row row = session().execute("SELECT token(i) FROM foo WHERE i = 1").one();
        Token expected = row.getToken(0);

        ProtocolVersion protocolVersion = cluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        assertThat(
                metadata.newToken(TypeCodec.cint().serialize(1, protocolVersion))
        ).isEqualTo(expected);
    }

    protected abstract Token.Factory tokenFactory();
}
