package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
    CCMBridge ccm;
    Cluster cluster;
    Session session;

    public TokenIntegrationTest(String ccmOptions, DataType expectedTokenType) {
        this.ccmOptions = ccmOptions;
        this.expectedTokenType = expectedTokenType;
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

    @Test(groups = "short")
    public void should_get_token_from_row_and_set_token_in_query() {
        // get by index:
        Row row = session.execute("SELECT token(i) FROM test.foo WHERE i = 1").one();
        Token token = row.getToken(0);
        assertThat(token.getType()).isEqualTo(expectedTokenType);

        // get by "base" column name (will add token() around it):
        assertThat(
            row.getToken("i")
        ).isEqualTo(token);

        // get by name:
        assertThat(
            session.execute("SELECT token(i) AS t FROM test.foo WHERE i = 1").one()
                .getToken("t")
        ).isEqualTo(token);

        // get by "base" column name when it's case-sensitive
        session.execute("SELECT token(\"caseSensitiveKey\") FROM test.foo2 WHERE \"caseSensitiveKey\" = 1").one()
            .getToken("\"caseSensitiveKey\"");


        PreparedStatement pst = session.prepare("SELECT * FROM test.foo WHERE token(i) = ?");
        row = session.execute(pst.bind(token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute(pst.bind().setToken(0, token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute("SELECT * FROM test.foo WHERE token(i) = ?", token).one();
        assertThat(row.getInt(0)).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_expose_token_ranges_per_host() {
        checkRangesPerHost("test", 1);
        checkRangesPerHost("test2", 2);
    }

    private void checkRangesPerHost(String keyspace, int replicationFactor) {
        Set<TokenRange> allRanges = Sets.newHashSet();

        // Get each host's ranges, the count should match the replication factor
        for (int i = 1; i <= 3; i++) {
            Host host = TestUtils.findHost(cluster, i);
            Set<TokenRange> hostRanges = cluster.getMetadata().getTokenRanges(keyspace, host);
            assertThat(hostRanges).hasSize(replicationFactor);
            allRanges.addAll(hostRanges);
        }

        // Once we ignore duplicates, the number of ranges should match the number of nodes.
        assertThat(allRanges).hasSize(3);
        Iterator<TokenRange> it = allRanges.iterator();
        TokenRange range1 = it.next();
        TokenRange range2 = it.next();
        TokenRange range3 = it.next();

        // No two ranges should intersect
        assertThat(range1)
            .doesNotIntersect(range2)
            .doesNotIntersect(range3);
        assertThat(range2)
            .doesNotIntersect(range3);

        // And the ranges should cover the whole ring
        TokenRange mergedRange = range1.mergeWith(range2.mergeWith(range3));
        boolean isFullRing = mergedRange.getStart().equals(mergedRange.getEnd())
            && !mergedRange.isEmpty();
        assertThat(isFullRing).isTrue();
    }

    @Test(groups = "short")
    public void should_expose_tokens_per_host() {
        for (Host host : cluster.getMetadata().allHosts()) {
            // We don't use virtual nodes in this test, so there is only one token
            assertThat(host.getTokens()).hasSize(1);
            Token tokenFromMetadata = host.getTokens().iterator().next();

            // Check against the info in the system tables, which is a bit weak since it's exactly how the metadata is
            // constructed in the first place, but there's not much else we can do.
            // Note that this relies on all queries going to node 1, which is why we use a WhiteList LBP in setup().
            Row row = (host.listenAddress == null)
                ? session.execute("select tokens from system.local").one()
                : session.execute("select tokens from system.peers where peer = ?", host.listenAddress).one();
            Set<String> tokenStrings = row.getSet("tokens", String.class);
            assertThat(tokenStrings).hasSize(1);
            Token tokenFromSystemTable = tokenFactory().fromString(tokenStrings.iterator().next());

            assertThat(tokenFromMetadata).isEqualTo(tokenFromSystemTable);
        }
    }

    protected abstract Token.Factory tokenFactory();
}
