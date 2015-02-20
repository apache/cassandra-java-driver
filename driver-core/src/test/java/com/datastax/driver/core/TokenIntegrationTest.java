package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * This class uses subclasses for each type of partitioner.
 *
 * There's normally a way to parametrize a TestNG class with @Factory and @DataProvider,
 * but it doesn't seem to work with multiple methods.
 */
public abstract class TokenIntegrationTest {

    List<String> schema = Lists.newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "CREATE KEYSPACE IF NOT EXISTS test2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}",
        "USE test",
        "CREATE TABLE IF NOT EXISTS foo(i int primary key)",
        "INSERT INTO foo (i) VALUES (1)",
        "INSERT INTO foo (i) VALUES (2)",
        "INSERT INTO foo (i) VALUES (3)",
        "CREATE TABLE IF NOT EXISTS foo2(\"caseSensitiveKey\" int primary key)",
        "INSERT INTO foo2 (\"caseSensitiveKey\") VALUES (1)"
    );

    private final String ccmOptions;
    private final DataType expectedTokenType;
    private final int numTokens;
    private final boolean useVnodes;
    CCMBridge ccm;
    Cluster cluster;
    Session session;

    public TokenIntegrationTest(String ccmOptions, DataType expectedTokenType) {
        this(ccmOptions, expectedTokenType, false);
    }

    public TokenIntegrationTest(String ccmOptions, DataType expectedTokenType, boolean useVnodes) {
        this.expectedTokenType = expectedTokenType;
        this.numTokens = useVnodes ? 256 : 1;
        this.ccmOptions = useVnodes ? ccmOptions + " --vnodes" : ccmOptions;
        this.useVnodes = useVnodes;
    }

    @BeforeClass(groups = "short")
    public void setup() {
        ccm = CCMBridge.create("test", 3, ccmOptions);

        // Only connect to node 1, which makes it easier to query system tables in should_expose_tokens_per_host()
        LoadBalancingPolicy lbp = new WhiteListPolicy(new RoundRobinPolicy(),
            Lists.newArrayList(new InetSocketAddress(CCMBridge.ipOfNode(1), 9042)));

        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withLoadBalancingPolicy(lbp)
            .build();
        cluster.init();
        session = cluster.connect();

        for (String statement : schema)
            session.execute(statement);
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster != null)
            cluster.close();
        if (ccm != null)
            ccm.remove();
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
        Metadata metadata = cluster.getMetadata();

        // Find the replica for a given partition key
        int testKey = 1;
        Set<Host> replicas = metadata.getReplicas("test", DataType.cint().serialize(testKey));
        assertThat(replicas).hasSize(1);
        Host replica = replicas.iterator().next();

        // Iterate the cluster's token ranges. For each one, use a range query to ask Cassandra which partition keys
        // are in this range.
        TokenRange foundRange = null;
        for (TokenRange range : metadata.getTokenRanges()) {
            List<Row> rows = rangeQuery("SELECT i FROM foo WHERE token(i) > ? and token(i) <= ?", range);
            for (Row row : rows) {
                if (row.getInt("i") == testKey) {
                    // We should find our test key exactly once
                    assertThat(foundRange)
                        .describedAs("found the same key in two ranges: " + foundRange + " and " + range)
                        .isNull();
                    foundRange = range;
                    // That range should be managed by the replica
                    assertThat(metadata.getReplicas("test", range)).contains(replica);
                }
            }
        }
        assertThat(foundRange).isNotNull();
    }

    private List<Row> rangeQuery(String query, TokenRange range) {
        List<Row> rows = Lists.newArrayList();
        for (TokenRange subRange : range.unwrap()) {
            rows.addAll(session.execute(query,
                subRange.getStart(), subRange.getEnd())
                .all());
        }
        return rows;
    }

    /**
     * <p>
     * Validates that a {@link Token} can be retrieved and parsed by executing 'select token(name)' and
     * then used to find data matching that token.
     * </p>
     *
     * <p>
     * This test does the following:
     *
     * <ol>
     *     <li>Retrieve the token for the key with value '1', get it by index, and ensure if is of the expected token type.</li>
     *     <li>Retrieve the token for the key with value '1', get it by name,  and ensure it matches the token by index.</li>
     *     <li>Retrieve the token by alias for the key '1', and ensure it matches the token by index.</li>
     *     <li>Retrieve the token for a case sensitive key.</li>
     *     <li>Select data by token with a BoundStatement.</li>
     *     <li>Select data by token using setToken by index.</li>
     *     <li>Select data by token using setToken by name.</li>
     *     <li>Select data by token with a SimpleStatement</li>
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
        Row row = session.execute("SELECT token(i) FROM test.foo WHERE i = 1").one();
        Token token = row.getToken(0);
        assertThat(token.getType()).isEqualTo(expectedTokenType);

        assertThat(
            row.getPartitionKeyToken()
        ).isEqualTo(token);

        // get by name when column is aliased:
        assertThat(
            session.execute("SELECT token(i) AS t FROM test.foo WHERE i = 1").one()
                .getToken("t")
        ).isEqualTo(token);


        PreparedStatement pst = session.prepare("SELECT * FROM test.foo WHERE token(i) = ?");
        row = session.execute(pst.bind(token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute(pst.bind().setToken(0, token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute(pst.bind().setPartitionKeyToken(token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        PreparedStatement pst2 = session.prepare("SELECT * FROM test.foo WHERE token(i) = :myToken");
        row = session.execute(pst2.bind().setToken("myToken", token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute("SELECT * FROM test.foo WHERE token(i) = ?", token).one();
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
        Row row = session.execute("SELECT i FROM test.foo WHERE i = 1").one();
        row.getToken(0);
    }

    /**
     * <p>
     * Ensures that @{link TokenRange}s are exposed at a per host level, the ranges are complete,
     * the entire ring is represented, and that ranges do not overlap.
     * </p>
     *
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
        checkRangesPerHost("test", 1);
        checkRangesPerHost("test2", 2);
        assertThat(cluster).hasValidTokenRanges();
    }

    private void checkRangesPerHost(String keyspace, int replicationFactor) {
        List<TokenRange> allRangesWithReplicas = Lists.newArrayList();

        // Get each host's ranges, the count should match the replication factor
        for (int i = 1; i <= 3; i++) {
            Host host = TestUtils.findHost(cluster, i);
            Set<TokenRange> hostRanges = cluster.getMetadata().getTokenRanges(keyspace, host);
            // Special case: When using vnodes the tokens are not evenly assigned to each replica.
            if(!useVnodes) {
                assertThat(hostRanges).hasSize(replicationFactor * numTokens);
            }
            allRangesWithReplicas.addAll(hostRanges);
        }

        // Special case check for vnodes to ensure that total number of replicated ranges is correct.
        assertThat(allRangesWithReplicas).hasSize(3 * numTokens * replicationFactor);

        // Once we ignore duplicates, the number of ranges should match the number of nodes.
        Set<TokenRange> allRanges = new HashSet<TokenRange>(allRangesWithReplicas);
        assertThat(allRanges).hasSize(3*numTokens);

        // And the ranges should cover the whole ring and no ranges intersect.
        assertThat(cluster).hasValidTokenRanges(keyspace);
    }

    /**
     * <p>
     * Ensures that Tokens are exposed for each Host and that the match those in the system tables.
     * </p>
     *
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
        for (Host host : cluster.getMetadata().allHosts()) {
            assertThat(host.getTokens()).hasSize(numTokens);

            // Check against the info in the system tables, which is a bit weak since it's exactly how the metadata is
            // constructed in the first place, but there's not much else we can do.
            // Note that this relies on all queries going to node 1, which is why we use a WhiteList LBP in setup().
            Row row = (host.listenAddress == null)
                ? session.execute("select tokens from system.local").one()
                : session.execute("select tokens from system.peers where peer = ?", host.listenAddress).one();
            Set<String> tokenStrings = row.getSet("tokens", String.class);
            assertThat(tokenStrings).hasSize(numTokens);
            Iterable<Token> tokensFromSystemTable = Iterables.transform(tokenStrings, new Function<String, Token>() {
                @Override public Token apply(String input) {
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
        Set<TokenRange> ranges = cluster.getMetadata().getTokenRanges();

        assertOnlyOneWrapped(ranges);

        Iterable<TokenRange> splitRanges = Iterables.concat(Iterables.transform(ranges,
            new Function<TokenRange, Iterable<TokenRange>>() {
                @Override public Iterable<TokenRange> apply(TokenRange input) {
                    return input.splitEvenly(10);
                }
            })
        );

        assertOnlyOneWrapped(splitRanges);
    }

    /**
     * Asserts that given the input {@link TokenRange}s that at most one of them wraps the token ring.
     * @param ranges Ranges to validate against.
     */
    protected void assertOnlyOneWrapped(Iterable<TokenRange> ranges) {
        TokenRange wrappedRange = null;

        for(TokenRange range : ranges) {
            if(range.isWrappedAround()) {
                assertThat(wrappedRange)
                    .as("Found a wrapped around TokenRange (%s) when one already exists (%s).", range, wrappedRange)
                    .isNull();
                wrappedRange = range;

                assertThat(range).unwrapsOverMinToken(tokenFactory());
            } else {
                assertThat(range).unwrapsToItself();
            }
        }
    }

    protected abstract Token.Factory tokenFactory();
}
