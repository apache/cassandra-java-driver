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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.mockito.Mockito.*;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createKeyspace = false)
public class NodeListRefreshDebouncerTest extends CCMTestsSupport {

    private static final int DEBOUNCE_TIME = 2000;

    private Cluster cluster2;

    // Control Connection to be spied.
    private ControlConnection controlConnection;

    @BeforeMethod(groups = "short")
    public void setup() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setRefreshNodeListIntervalMillis(DEBOUNCE_TIME);
        queryOptions.setMaxPendingRefreshNodeListRequests(5);
        queryOptions.setRefreshSchemaIntervalMillis(0);
        // Create a separate cluster that will receive the schema events on its control connection.
        cluster2 = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withQueryOptions(queryOptions)
                .build());

        cluster2.init();

        // Create a spy of the Cluster's control connection and replace it with the spy.
        controlConnection = spy(cluster2.manager.controlConnection);
        cluster2.manager.controlConnection = controlConnection;
        reset(controlConnection);
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
        for (int i = 0; i < 5; i++) {
            cluster2.manager.submitNodeListRefresh();
        }

        // add delay to account for executor submit.
        verify(controlConnection, timeout(DEBOUNCE_TIME)).refreshNodeListAndTokenMap();
    }
}
