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

import java.util.Collection;

import org.testng.annotations.*;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;

public class NodeListRefreshDebouncerTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final int DEBOUNCE_TIME = 2000;

    QueryOptions queryOptions;

    Cluster cluster2;

    Session session2;

    // Control Connection to be spied.
    private ControlConnection controlConnection;

    // Keyspaces to drop after test completes.
    private Collection<String> keyspaces = newArrayList();

    @Override
    protected Collection<String> getTableDefinitions() {
        return newArrayList();
    }

    @BeforeClass(groups = "short")
    public void setup() {
        queryOptions = new QueryOptions();
        queryOptions.setRefreshNodeListIntervalMillis(DEBOUNCE_TIME);
        queryOptions.setMaxPendingRefreshNodeListRequests(5);
        queryOptions.setRefreshSchemaIntervalMillis(0);
        // Create a separate cluster that will receive the schema events on its control connection.
        cluster2 = this.configure(Cluster.builder())
            .addContactPointsWithPorts(newArrayList(hostAddress))
            .withQueryOptions(queryOptions)
            .build();
        session2 = cluster2.connect();

        // Create a spy of the Cluster's control connection and replace it with the spy.
        controlConnection = spy(cluster2.manager.controlConnection);
        cluster2.manager.controlConnection = controlConnection;
        reset(controlConnection);
    }

    @AfterMethod(groups = "short")
    public void beforeMethod() {
        reset(controlConnection);
    }

    @AfterClass(groups = "short")
    public void teardown() {
        cluster2.close();
        // drop all keyspaces
        for(String keyspace : keyspaces) {
            session.execute("DROP KEYSPACE " + keyspace);
        }
    }

    /**
     * Ensures that when a keyspace is created that refresh node list request
     * is debounced and processed within {@link QueryOptions#getRefreshSchemaIntervalMillis()}
     * + {@link QueryOptions#getRefreshNodeListIntervalMillis()}.
     *
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_refresh_when_keyspace_created() {
        String keyspace = "sdrwkc";
        session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
        keyspaces.add(keyspace);

        verify(controlConnection, timeout(DEBOUNCE_TIME + queryOptions.getRefreshSchemaIntervalMillis())).refreshNodeListAndTokenMap();
    }

    /**
     * Ensures that when enough refreshNodeList requests have been received
     * to reach {@link QueryOptions#getMaxPendingRefreshNodeListRequests()} that a
     * node list refresh is submitted right away.
     *
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_refresh_when_max_pending_requests_reached() {
        // Create keyspaces 5 times to cause
        // refreshNodeListAndTokenMap to be called.
        String prefix = "srwmprr";
        for(int i = 0; i < 5; i++) {
            String keyspace = prefix + i;
            session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
            keyspaces.add(keyspace);
        }

        // add a 1 second delay to account for executor submit.
        verify(controlConnection, timeout(1000)).refreshNodeListAndTokenMap();
    }
}
