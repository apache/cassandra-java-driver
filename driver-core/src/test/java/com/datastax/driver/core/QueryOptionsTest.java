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

import com.google.common.collect.Lists;
import org.scassandra.http.client.PreparedStatementPreparation;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

public class QueryOptionsTest {

    ScassandraCluster scassandra;

    QueryOptions queryOptions;

    SortingLoadBalancingPolicy loadBalancingPolicy;

    Cluster cluster = null;
    Session session = null;
    Host host1, host2, host3;


    @BeforeMethod(groups = "short")
    public void beforeMethod() {
        scassandra = ScassandraCluster.builder().withNodes(3).build();
        scassandra.init();

        queryOptions = new QueryOptions();
        loadBalancingPolicy = new SortingLoadBalancingPolicy();
        cluster = Cluster.builder()
                .addContactPoints(scassandra.address(2).getAddress())
                .withPort(scassandra.getBinaryPort())
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withQueryOptions(queryOptions)
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        // Make sure there are no prepares
        for (int host : Lists.newArrayList(1, 2, 3)) {
            assertThat(scassandra.node(host).activityClient().retrievePreparedStatementPreparations()).hasSize(0);
            scassandra.node(host).activityClient().clearAllRecordedActivity();
        }
    }

    public void validatePrepared(boolean expectAll) {
        // Prepare the statement
        String query = "select sansa_stark from the_known_world";
        PreparedStatement statement = session.prepare(query);
        assertThat(cluster.manager.preparedQueries).containsValue(statement);

        // Ensure prepared properly based on expectation.
        List<PreparedStatementPreparation> preparationOne = scassandra.node(1).activityClient().retrievePreparedStatementPreparations();
        List<PreparedStatementPreparation> preparationTwo = scassandra.node(2).activityClient().retrievePreparedStatementPreparations();
        List<PreparedStatementPreparation> preparationThree = scassandra.node(3).activityClient().retrievePreparedStatementPreparations();

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

    private void valideReprepareOnUp(boolean expectReprepare) {
        String query = "select sansa_stark from the_known_world";
        int maxTries = 3;
        for (int i = 1; i <= maxTries; i++) {
            session.prepare(query);

            List<PreparedStatementPreparation> preparationOne = scassandra.node(1).activityClient().retrievePreparedStatementPreparations();

            assertThat(preparationOne).hasSize(1);
            assertThat(preparationOne.get(0).getPreparedStatementText()).isEqualTo(query);

            scassandra.node(1).activityClient().clearAllRecordedActivity();
            scassandra.node(1).stop();
            assertThat(cluster).host(1).goesDownWithin(10, TimeUnit.SECONDS);

            scassandra.node(1).start();
            assertThat(cluster).host(1).comesUpWithin(60, TimeUnit.SECONDS);

            preparationOne = scassandra.node(1).activityClient().retrievePreparedStatementPreparations();
            if (expectReprepare) {
                // tests fail randomly at this point, probably due to
                // https://github.com/scassandra/scassandra-server/issues/116
                try {
                    assertThat(preparationOne).hasSize(1);
                    assertThat(preparationOne.get(0).getPreparedStatementText()).isEqualTo(query);
                    break;
                } catch (AssertionError e) {
                    if (i == maxTries)
                        throw e;
                    // retry
                    scassandra.node(1).activityClient().clearAllRecordedActivity();
                }
            } else {
                assertThat(preparationOne).isEmpty();
                break;
            }
        }
    }

    /**
     * <p>
     * Validates that statements are reprepared when a node comes back up when
     * {@link QueryOptions#setReprepareOnUp(boolean)} is set to true.
     * </p>
     *
     * @test_category prepared_statements:prepared
     * @expected_result reprepare query on the restarted node.
     * @jira_ticket JAVA-658
     * @since 2.0.11, 2.1.8, 2.2.1
     */
    @Test(groups = "short")
    public void should_reprepare_on_up_when_enabled() {
        queryOptions.setReprepareOnUp(true);
        valideReprepareOnUp(true);
    }

    /**
     * <p>
     * Validates that statements are reprepared when a node comes back up with
     * the default configuration.
     * </p>
     *
     * @test_category prepared_statements:prepared
     * @expected_result reprepare query on the restarted node.
     * @jira_ticket JAVA-658
     * @since 2.0.11, 2.1.8, 2.2.1
     */
    @Test(groups = "short")
    public void should_reprepare_on_up_by_default() {
        valideReprepareOnUp(true);
    }

    /**
     * <p>
     * Validates that statements are not reprepared when a node comes back up when
     * {@link QueryOptions#setReprepareOnUp(boolean)} is set to false.
     * </p>
     *
     * @test_category prepared_statements:prepared
     * @expected_result do not reprepare query on the restarted node.
     * @jira_ticket JAVA-658
     * @since 2.0.11, 2.1.8, 2.2.1
     */
    @Test(groups = "short")
    public void should_not_reprepare_on_up_when_disabled() {
        queryOptions.setReprepareOnUp(false);
        valideReprepareOnUp(false);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void afterMethod() {
        if (cluster != null)
            cluster.close();

        if (scassandra != null)
            scassandra.stop();
    }
}
