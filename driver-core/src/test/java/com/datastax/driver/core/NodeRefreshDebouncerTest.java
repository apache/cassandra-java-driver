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

import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.*;

@CCMConfig(dirtiesContext = true, createCluster = false)
public class NodeRefreshDebouncerTest extends CCMTestsSupport {

    /**
     * Ensures that when a new node is bootstrapped into the cluster, stopped, and then subsequently
     * started within {@link QueryOptions#setRefreshNodeIntervalMillis(int)} that an 'onAdd' event
     * event is the only one processed and that the {@link Host} is marked up.
     * <p/>
     * Since NEW_NODE_DELAY_SECONDS is typically configured with a high value (60 seconds default
     * in the maven profile) this test can take a very long time.
     *
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "long")
    public void should_call_onAdd_with_bootstrap_stop_start() {
        int refreshNodeInterval = 30000;
        QueryOptions queryOptions = new QueryOptions().setRefreshNodeIntervalMillis(refreshNodeInterval);
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withQueryOptions(queryOptions)
                .build());
        cluster.connect();
        Host.StateListener listener = mock(Host.StateListener.class);
        cluster.register(listener);
        ccm().add(2);
        ccm().start(2);
        ccm().stop(2);
        ccm().start(2);

        ArgumentCaptor<Host> captor = forClass(Host.class);

        // Only register and onAdd should be called, since stop and start should be discarded.
        verify(listener).onRegister(cluster);
        long addDelay = refreshNodeInterval + TimeUnit.MILLISECONDS.convert(Cluster.NEW_NODE_DELAY_SECONDS, TimeUnit.SECONDS);
        verify(listener, timeout(addDelay)).onAdd(captor.capture());
        verifyNoMoreInteractions(listener);

        // The hosts state should be UP.
        assertThat(captor.getValue().getState()).isEqualTo("UP");
        assertThat(cluster).host(2).hasState(Host.State.UP);
    }
}
