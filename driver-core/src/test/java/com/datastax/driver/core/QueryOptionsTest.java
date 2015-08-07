/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.List;
import com.google.common.collect.Lists;
import org.scassandra.http.client.PreparedStatementPreparation;
import org.testng.annotations.*;

import static com.datastax.driver.core.Assertions.assertThat;

public class QueryOptionsTest {
    SCassandraCluster scassandra;

    QueryOptions queryOptions = new QueryOptions();

    SortingLoadBalancingPolicy loadBalancingPolicy;

    Cluster cluster = null;
    Session session = null;
    Host host1, host2, host3;


    @BeforeClass(groups = "short")
    public void beforeClass() {
        scassandra = new SCassandraCluster(CCMBridge.IP_PREFIX, 3);
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod() {
        loadBalancingPolicy = new SortingLoadBalancingPolicy();
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(2))
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withQueryOptions(queryOptions)
            .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        // Make sure there are no prepares
        for(int host : Lists.newArrayList(1, 2, 3)) {
            assertThat(scassandra.retrievePreparedStatementPreparations(host)).hasSize(0);
        }
    }

    public void validatePrepared(boolean expectAll) {
        // Prepare the statement
        String query = "select sansa_stark from the_known_world";
        session.prepare(query);

        // Ensure prepared properly based on expectation.
        List<PreparedStatementPreparation> preparationOne = scassandra.retrievePreparedStatementPreparations(1);
        List<PreparedStatementPreparation> preparationTwo = scassandra.retrievePreparedStatementPreparations(2);
        List<PreparedStatementPreparation> preparationThree = scassandra.retrievePreparedStatementPreparations(3);

        assertThat(preparationOne).hasSize(1);
        assertThat(preparationOne.get(0).getPreparedStatementText()).isEqualTo(query);

        if (expectAll) {
            assertThat(preparationTwo).hasSize(1);
            assertThat(preparationTwo.get(0).getPreparedStatementText()).isEqualTo(query);
            assertThat(preparationThree).hasSize(1);
            assertThat(preparationThree.get(0).getPreparedStatementText()).isEqualTo(query);
        } else {
            assertThat(preparationTwo).isEmpty();
            assertThat(preparationThree).isEmpty();
        }
    }

    /**
     * <p>
     * Validates that statements are only prepared on one node when
     * {@link QueryOptions#setPrepareOnAllHosts(boolean)} is set to false.
     * </p>
     *
     * @test_category prepared_statements:prepared
     * @expected_result prepare query only on the first node.
     * @jira_ticket JAVA-797
     * @since 2.0.11, 2.1.8, 2.2.1
     */
    @Test(groups = "short")
    public void should_prepare_once_when_prepare_on_all_hosts_false() {
        queryOptions.setPrepareOnAllHosts(false);
        validatePrepared(false);
    }

    /**
     * <p>
     * Validates that statements are prepared on one node when
     * {@link QueryOptions#setPrepareOnAllHosts(boolean)} is set to true.
     * </p>
     *
     * @test_category prepared_statements:prepared
     * @expected_result all nodes prepared the query
     * @jira_ticket JAVA-797
     * @since 2.0.11, 2.1.8, 2.2.1
     */
    @Test(groups = "short")
    public void should_prepare_everywhere_when_prepare_on_all_hosts_true() {
        queryOptions.setPrepareOnAllHosts(true);
        validatePrepared(true);
    }

    /**
     * <p>
     * Validates that statements are prepared on one node when
     * {@link QueryOptions#setPrepareOnAllHosts(boolean)} is not set.
     * </p>
     *
     * @test_category prepared_statements:prepared
     * @expected_result all nodes prepared the query.
     * @jira_ticket JAVA-797
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_prepare_everywhere_when_not_configured() {
        validatePrepared(true);
    }

    @AfterMethod(groups = "short")
    public void afterMethod() {
        scassandra.clearAllPrimes();
        scassandra.clearAllRecordedActivity();
    }

    @AfterClass(groups = "short")
    public void afterClass() {
        if (scassandra != null)
            scassandra.stop();
    }
}
