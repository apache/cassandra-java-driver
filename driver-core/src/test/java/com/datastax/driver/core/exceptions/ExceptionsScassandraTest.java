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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.scassandra.Scassandra;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.testng.annotations.*;

import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.*;

public class ExceptionsScassandraTest {

    protected ScassandraCluster scassandras;
    protected Cluster cluster;
    protected Metrics.Errors errors;
    protected Host host1;
    protected Session session;

    @BeforeClass(groups = "short")
    public void beforeClass() {
        scassandras = ScassandraCluster.builder().withNodes(1).build();
        scassandras.init();
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod() {
        cluster = Cluster.builder()
                .addContactPoints(scassandras.address(1).getAddress())
                .withPort(scassandras.getBinaryPort())
                .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                .build();
        session = cluster.connect();
        host1 = TestUtils.findHost(cluster, 1);
        errors = cluster.getMetrics().getErrorMetrics();

        for (Scassandra node : scassandras.nodes()) {
            node.primingClient().clearAllPrimes();
            node.activityClient().clearAllRecordedActivity();
        }
    }

    @Test(groups = "short")
    public void should_throw_proper_unavailable_exception() {
        simulateError(1, unavailable);
        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {
            assertThat(e.getMessage()).isEqualTo("Not enough replicas available for query at consistency LOCAL_ONE (1 required but only 0 alive)");
            assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
            assertThat(e.getAliveReplicas()).isEqualTo(0);
            assertThat(e.getRequiredReplicas()).isEqualTo(1);
            assertThat(e.getAddress()).isEqualTo(host1.getSocketAddress());
            assertThat(e.getHost()).isEqualTo(host1.getAddress());
        }
    }

    @Test(groups = "short")
    public void should_throw_proper_read_timeout_exception() {
        simulateError(1, read_request_timeout);
        try {
            query();
            fail("expected a ReadTimeoutException");
        } catch (ReadTimeoutException e) {
            assertThat(e.getMessage()).isEqualTo("Cassandra timeout during read query at consistency LOCAL_ONE (1 responses were required but only 0 replica responded)");
            assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
            assertThat(e.getReceivedAcknowledgements()).isEqualTo(0);
            assertThat(e.getRequiredAcknowledgements()).isEqualTo(1);
            assertThat(e.getAddress()).isEqualTo(host1.getSocketAddress());
            assertThat(e.getHost()).isEqualTo(host1.getAddress());
        }
    }

    @Test(groups = "short")
    public void should_throw_proper_write_timeout_exception() {
        simulateError(1, write_request_timeout);
        try {
            query();
            fail("expected a WriteTimeoutException");
        } catch (WriteTimeoutException e) {
            assertThat(e.getMessage()).isEqualTo("Cassandra timeout during write query at consistency LOCAL_ONE (1 replica were required but only 0 acknowledged the write)");
            assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
            assertThat(e.getReceivedAcknowledgements()).isEqualTo(0);
            assertThat(e.getRequiredAcknowledgements()).isEqualTo(1);
            assertThat(e.getWriteType()).isEqualTo(WriteType.SIMPLE);
            assertThat(e.getAddress()).isEqualTo(host1.getSocketAddress());
            assertThat(e.getHost()).isEqualTo(host1.getAddress());
        }
    }

    protected void simulateError(int hostNumber, Result result) {
        scassandras.node(hostNumber).primingClient().prime(PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withThen(then().withResult(result))
                .build());
    }

    private static List<Map<String, ?>> row(String key, String value) {
        return ImmutableList.<Map<String, ?>>of(ImmutableMap.of(key, value));
    }

    protected ResultSet query() {
        return query(session);
    }

    protected ResultSet query(Session session) {
        return session.execute("mock query");
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void afterMethod() {
        for (Scassandra node : scassandras.nodes()) {
            node.primingClient().clearAllPrimes();
        }
        if (cluster != null)
            cluster.close();
    }

    @AfterClass(groups = "short", alwaysRun = true)
    public void afterClass() {
        if (scassandras != null)
            scassandras.stop();
    }
}
