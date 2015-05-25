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

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.datastax.driver.core.ProtocolVersion.V4;

public class UnsetValuesTest {

    private CCMBridge ccm;
    private Cluster cluster;
    private PreparedStatement prepared;
    private Session session;

    @BeforeClass
    public void createCcm() throws Exception {
        ccm = CCMBridge.create("test", 1);
    }

    @AfterClass
    public void closeCcm() throws Exception {
        if(ccm != null) ccm.remove();
    }

    public void buildCluster(ProtocolVersion version) {
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.IP_PREFIX + 1)
            .withProtocolVersion(version).build();
        session = cluster.newSession();
        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        session.execute("USE ks");
        session.execute("CREATE TABLE IF NOT EXISTS foo (c1 int primary key, c2 text)");
        prepared = session.prepare("INSERT INTO foo (c1, c2) VALUES (?, ?)");
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        st1.setString(1, "foo");
        session.execute(st1);
    }

    @AfterMethod
    public void closeCluster() throws Exception {
        if(cluster != null) cluster.close();
    }

    @Test(groups = "short")
    public void should_not_allow_unset_value_on_bound_statement_when_protocol_lesser_than_v4() {
        buildCluster(V3);
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        // c2 is UNSET
        try {
            session.execute(st1);
            fail("Should not have executed statement with UNSET values in protocol V3");
        } catch(IllegalStateException e) {
            assertThat(e.getMessage()).contains("Unset value at index 1");
        }
    }

    @Test(groups = "short")
    public void should_not_allow_unset_value_on_batch_statement_when_protocol_lesser_than_v4() {
        buildCluster(V3);
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        // c2 is UNSET
        try {
            session.execute(new BatchStatement().add(st1));
            fail("Should not have executed statement with UNSET values in protocol V3");
        } catch(IllegalStateException e) {
            assertThat(e.getMessage()).contains("Unset value at index 1");
        }
    }

    @Test(groups = "short")
    public void should_not_create_tombstone_when_unset_value_on_bound_statement_and_protocol_v4() {
        buildCluster(V4);
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        // c2 is UNSET
        session.execute(st1);
        Statement st2 = new SimpleStatement("SELECT c2 from foo where c1 = 42");
        st2.enableTracing();
        ResultSet rows = session.execute(st2);
        assertThat(rows.one().getString("c2")).isEqualTo("foo");
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        checkEventsContain(queryTrace, "0 tombstone");
        System.out.println(queryTrace.getEvents());
    }

    @Test(groups = "short")
    public void should_not_create_tombstone_when_unset_value_on_batch_statement_and_protocol_v4() {
        buildCluster(V4);
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        // c2 is UNSET
        session.execute(new BatchStatement().add(st1));
        Statement st2 = new SimpleStatement("SELECT c2 from foo where c1 = 42");
        st2.enableTracing();
        ResultSet rows = session.execute(st2);
        assertThat(rows.one().getString("c2")).isEqualTo("foo");
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        checkEventsContain(queryTrace, "0 tombstone");
        System.out.println(queryTrace.getEvents());
    }

    @Test(groups = "short")
    public void should_create_tombstone_when_null_value_on_bound_statement() {
        buildCluster(V4);
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        st1.setString(1, null);
        session.execute(st1);
        Statement st2 = new SimpleStatement("SELECT c2 from foo where c1 = 42");
        st2.enableTracing();
        ResultSet rows = session.execute(st2);
        assertThat(rows.one().getString("c2")).isNull();
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        checkEventsContain(queryTrace, "1 tombstone");
        System.out.println(queryTrace.getEvents());
    }

    @Test(groups = "short")
    public void should_create_tombstone_when_null_value_on_batch_statement() {
        buildCluster(V4);
        BoundStatement st1 = prepared.bind();
        st1.setInt(0, 42);
        st1.setString(1, null);
        session.execute(new BatchStatement().add(st1));
        Statement st2 = new SimpleStatement("SELECT c2 from foo where c1 = 42");
        st2.enableTracing();
        ResultSet rows = session.execute(st2);
        assertThat(rows.one().getString("c2")).isNull();
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        checkEventsContain(queryTrace, "1 tombstone");
        System.out.println(queryTrace.getEvents());
    }

    private boolean checkEventsContain(QueryTrace queryTrace, String toFind){
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            if(event.getDescription().contains(toFind)) return true;
        }
        return false;
    }

}