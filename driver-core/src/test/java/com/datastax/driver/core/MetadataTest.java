package com.datastax.driver.core;

import java.util.Map;

import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.waitFor;

public class MetadataTest {

    /**
     * <p>
     * Validates that when the topology of the cluster changes that Cluster Metadata is properly updated.
     * </p>
     *
     * <p>
     * This test does the following:
     *
     * <ol>
     *     <li>Creates a 3 node cluster and capture token range data from the {@link Cluster}'s {@link Metadata}</li>
     *     <li>Decommission node 3.</li>
     *     <li>Validates that the token range data was updated to reflect node 3 leaving and the other nodes
     *     taking on its token range.</li>
     *     <li>Adds a new node, node 4.</li>
     *     <li>Validates that the token range data was updated to reflect node 4 joining the {@link Cluster} and
     *     the token ranges reflecting this.</li>
     * </ol>
     *
     * @test_category metadata:token
     * @expected_result cluster metadata is properly updated in response to node remove and add events.
     * @jira_ticket JAVA-312
     * @since 2.0.10, 2.1.5
     */
    @Test(groups="long")
    public void should_update_metadata_on_topology_change() {
        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            ccm = CCMBridge.create("test", 3);

            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .build();
            Session session = cluster.connect();

            String keyspace = "test";
            session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            Metadata metadata = cluster.getMetadata();

            // Capture all Token data.
            assertThat(metadata.getTokenRanges()).hasSize(3);
            Map<Host, Token> tokensForHost = getTokenForHosts(metadata);

            // Capture host3s token and range before we take it down.
            Host host3 = TestUtils.findHost(cluster, 3);
            Token host3Token = tokensForHost.get(host3);

            ccm.decommissionNode(3);
            ccm.remove(3);

            // Ensure that the token ranges were updated, there should only be 2 ranges now.
            assertThat(metadata.getTokenRanges()).hasSize(2);

            // The token should not be present for any Host.
            assertThat(getTokenForHosts(metadata)).doesNotContainValue(host3Token);

            // The ring should be fully accounted for.
            assertThat(cluster).hasValidTokenRanges("test");
            assertThat(cluster).hasValidTokenRanges();

            // Add an additional node.
            ccm.bootstrapNode(4);
            waitFor(CCMBridge.IP_PREFIX + '4', cluster);

            // Ensure that the token ranges were updated, there should only be 3 ranges now.
            assertThat(metadata.getTokenRanges()).hasSize(3);

            Host host4 = TestUtils.findHost(cluster, 4);
            TokenRange host4Range = metadata.getTokenRanges(keyspace, host4).iterator().next();

            // Ensure no host token range intersects with node 4.
            for(Host host : metadata.getAllHosts()) {
                if(!host.equals(host4)) {
                    TokenRange hostRange = metadata.getTokenRanges(keyspace, host).iterator().next();
                    assertThat(host4Range).doesNotIntersect(hostRange);
                }
            }

            // The ring should be fully accounted for.
            assertThat(cluster).hasValidTokenRanges("test");
            assertThat(cluster).hasValidTokenRanges();
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * @return A mapping of Host -> Token for each Host in the given {@link Metadata}
     */
    private Map<Host, Token> getTokenForHosts(Metadata metadata) {
        Map<Host, Token> tokensByHost = Maps.newHashMap();
        for(Host host : metadata.getAllHosts()) {
            tokensByHost.put(host, host.getTokens().iterator().next());
        }
        return tokensByHost;
    }
}
