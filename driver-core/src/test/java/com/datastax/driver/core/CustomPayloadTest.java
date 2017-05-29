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

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.apache.log4j.Level.TRACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@CassandraVersion("2.2.0")
@CCMConfig(jvmArgs = "-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler")
public class CustomPayloadTest extends CCMTestsSupport {

    private Map<String, ByteBuffer> payload1;

    private Map<String, ByteBuffer> payload2;

    @BeforeMethod(groups = {"short", "unit"})
    public void initPayloads() {
        payload1 = ImmutableMap.of(
                "k1", ByteBuffer.wrap(new byte[]{1, 2, 3}),
                "k2", ByteBuffer.wrap(new byte[]{4, 5, 6})
        );
        payload2 = ImmutableMap.of(
                "k2", ByteBuffer.wrap(new byte[]{1, 2}),
                "k3", ByteBuffer.wrap(new byte[]{3, 4})
        );
    }

    // execute

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_statement() throws Exception {
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setOutgoingPayload(payload1);
        ResultSet rows = session().execute(statement);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload1);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_batch_statement() throws Exception {
        Statement statement = new BatchStatement().add(new SimpleStatement("INSERT INTO t1 (c1, c2) values (1, 'foo')"));
        statement.setOutgoingPayload(payload1);
        ResultSet rows = session().execute(statement);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload1);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_building_statement() throws Exception {
        Statement statement = select("c2").from("t1").where(eq("c1", 1)).setOutgoingPayload(payload1);
        ResultSet rows = session().execute(statement);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload1);
    }

    // prepare

    /**
     * Ensures that an incoming payload is propagated from prepared to bound statements.
     */
    @Test(groups = "short")
    public void should_propagate_incoming_payload_to_bound_statement() throws Exception {
        RegularStatement statement = new SimpleStatement("SELECT c2 as col1 FROM t1 where c1 = ?");
        statement.setOutgoingPayload(payload1);
        PreparedStatement ps = session().prepare(statement);
        // Prepared statement should inherit outgoing payload
        assertThat(ps.getOutgoingPayload()).isEqualTo(payload1);
        // Prepared statement should receive incoming payload
        assertThat(ps.getIncomingPayload()).isEqualTo(payload1);
        ps.setOutgoingPayload(null); // unset outgoing payload
        // bound statement should inherit from prepared statement's incoming payload
        BoundStatement bs = ps.bind(1);
        ResultSet rows = session().execute(bs);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload1);
        bs = ps.bind();
        bs.setInt(0, 1);
        rows = session().execute(bs);
        actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload1);
    }

    /**
     * Ensures that an incoming payload is overridden by an explicitly set outgoing payload
     * when propagated to bound statements.
     */
    @Test(groups = "short")
    public void should_override_incoming_payload_when_outgoing_payload_explicitly_set_on_preparing_statement() throws Exception {
        RegularStatement statement = new SimpleStatement("SELECT c2 as col2 FROM t1 where c1 = ?");
        statement.setOutgoingPayload(payload1);
        PreparedStatement ps = session().prepare(statement);
        // Prepared statement should inherit outgoing payload
        assertThat(ps.getOutgoingPayload()).isEqualTo(payload1);
        // Prepared statement should receive incoming payload
        assertThat(ps.getIncomingPayload()).isEqualTo(payload1);
        ps.setOutgoingPayload(payload2); // override outgoing payload
        // bound statement should inherit from prepared statement's outgoing payload
        BoundStatement bs = ps.bind(1);
        ResultSet rows = session().execute(bs);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload2);
        bs = ps.bind();
        bs.setInt(0, 1);
        rows = session().execute(bs);
        actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload2);
    }

    /**
     * Ensures that payloads can still be set individually on bound statements
     * if the prepared statement does not have a default payload.
     */
    @Test(groups = "short")
    public void should_not_set_any_payload_on_bound_statement() throws Exception {
        RegularStatement statement = new SimpleStatement("SELECT c2 as col3 FROM t1 where c1 = ?");
        PreparedStatement ps = session().prepare(statement);
        assertThat(ps.getOutgoingPayload()).isNull();
        assertThat(ps.getIncomingPayload()).isNull();
        // bound statement should not have outgoing payload
        BoundStatement bs = ps.bind(1);
        assertThat(bs.getOutgoingPayload()).isNull();
        // explicitly set a payload for this boudn statement only
        bs.setOutgoingPayload(payload1);
        ResultSet rows = session().execute(bs);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload1);
        // a second bound statement should not have any payload
        bs = ps.bind();
        assertThat(bs.getOutgoingPayload()).isNull();
        bs.setInt(0, 1);
        rows = session().execute(bs);
        actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isNull();
    }

    // pagination

    /**
     * Ensures that a custom payload is propagated throughout pages.
     */
    @Test(groups = "short")
    public void should_echo_custom_payload_when_paginating() throws Exception {
        session().execute("INSERT INTO t1 (c1, c2) VALUES (1, 'a')");
        session().execute("INSERT INTO t1 (c1, c2) VALUES (1, 'b')");
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = 1");
        statement.setFetchSize(1);
        statement.setOutgoingPayload(payload1);
        ResultSet rows = session().execute(statement);
        rows.all();
        assertThat(rows.getAllExecutionInfo()).extracting("incomingPayload").containsOnly(payload1);
    }

    // TODO retries, spec execs

    // edge cases

    @Test(groups = "short")
    public void should_encode_null_values() throws Exception {
        Map<String, ByteBuffer> payload = new HashMap<String, ByteBuffer>();
        payload.put("k1", Statement.NULL_PAYLOAD_VALUE);
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setOutgoingPayload(payload);
        ResultSet rows = session().execute(statement);
        Map<String, ByteBuffer> actual = rows.getExecutionInfo().getIncomingPayload();
        assertThat(actual).isEqualTo(payload);
    }

    @Test(groups = "unit", expectedExceptions = NullPointerException.class)
    public void should_throw_npe_when_null_key_on_regular_statement() throws Exception {
        Map<String, ByteBuffer> payload = new HashMap<String, ByteBuffer>();
        payload.put(null, ByteBuffer.wrap(new byte[]{1}));
        new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1).setOutgoingPayload(payload);
    }

    @Test(groups = "unit", expectedExceptions = NullPointerException.class)
    public void should_throw_npe_when_null_value_on_regular_statement() throws Exception {
        Map<String, ByteBuffer> payload = new HashMap<String, ByteBuffer>();
        payload.put("k1", null);
        new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1).setOutgoingPayload(payload);
    }

    @Test(groups = "short", expectedExceptions = NullPointerException.class)
    public void should_throw_npe_when_null_key_on_prepared_statement() throws Exception {
        Map<String, ByteBuffer> payload = new HashMap<String, ByteBuffer>();
        payload.put(null, ByteBuffer.wrap(new byte[]{1}));
        session().prepare(new SimpleStatement("SELECT c2 FROM t1 where c1 = 1")).setOutgoingPayload(payload);
    }

    @Test(groups = "short", expectedExceptions = NullPointerException.class)
    public void should_throw_npe_when_null_value_on_prepared_statement() throws Exception {
        Map<String, ByteBuffer> payload = new HashMap<String, ByteBuffer>();
        payload.put("k1", null);
        session().prepare(new SimpleStatement("SELECT c2 FROM t1 where c1 = 2")).setOutgoingPayload(payload);
    }

    @Test(groups = "short")
    public void should_throw_ufe_when_protocol_version_lesser_than_4() throws Exception {
        try {
            Cluster v3cluster = register(Cluster.builder()
                    .addContactPoints(getContactPoints())
                    .withPort(ccm().getBinaryPort())
                    .withProtocolVersion(V3)
                    .build())
                    .init();
            Session v3session = v3cluster.connect();
            Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
            statement.setOutgoingPayload(payload1);
            v3session.execute(statement);
            fail("Should not send custom payloads with protocol V3");
        } catch (UnsupportedFeatureException e) {
            assertThat(e.getMessage()).isEqualTo(
                    "Unsupported feature with the native protocol V3 (which is currently in use): Custom payloads are only supported since native protocol V4");
        }
    }

    // log messages

    /**
     * Ensures that when debugging custom payloads, the driver will print appropriate log messages.
     */
    @Test(groups = "short")
    public void should_print_log_message_when_level_trace() throws Exception {
        Logger logger = Logger.getLogger(Message.logger.getName());
        MemoryAppender appender = new MemoryAppender();
        try {
            logger.setLevel(TRACE);
            logger.addAppender(appender);
            Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
            statement.setOutgoingPayload(payload1);
            session().execute(statement);
            String logs = appender.waitAndGet(10000);
            assertThat(logs)
                    .contains("Sending payload: {k1:0x010203, k2:0x040506} (20 bytes total)")
                    .contains("Received payload: {k1:0x010203, k2:0x040506} (20 bytes total)");
        } finally {
            logger.setLevel(null);
            logger.removeAppender(appender);
        }
    }

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE t1 (c1 int, c2 text,  PRIMARY KEY (c1, c2))");
    }

}
