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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.datastax.driver.core.TypeCodecTest.User;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class TypeCodecJsonIntegrationTest {

    private CCMBridge ccm;
    private Cluster cluster;
    private Session session;

    private List<String> schema = Lists.newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE test",
        "CREATE TABLE IF NOT EXISTS \"myTable\" (c1 text, c2 text, c3 list<text>, PRIMARY KEY (c1, c2))"
    );

    private final JsonCodec<User> jsonCodec = new JsonCodec<User>(User.class);

    private final User alice = new User(1, "Alice");
    private final User bob = new User(2, "Bob");
    private final User charlie = new User(3, "Charlie");

    private final String bobJson = "{\"id\":2,\"name\":\"Bob\"}";
    private final String charlieJson = "{\"id\":3,\"name\":\"Charlie\"}";
    private final String aliceJson = "{\"id\":1,\"name\":\"Alice\"}";

    private final ArrayList<User> bobAndCharlie = Lists.newArrayList(bob, charlie);

    private final String insertQuery = "INSERT INTO \"myTable\" (c1, c2, c3) VALUES (?, ?, ?)";
    private final String selectQuery = "SELECT c1, c2, c3 FROM \"myTable\" WHERE c1 = ? and c2 = ?";
    private final String notAJsonString = "this text is not json";

    @BeforeClass(groups = "short")
    public void setupCcm() {
        ccm = CCMBridge.create("test", 1);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void teardown() {
        if (cluster != null)
            cluster.close();
    }

    @AfterClass(groups = "short", alwaysRun = true)
    public void teardownCcm() {
        if (ccm != null)
            ccm.remove();
    }

    @Test(groups = "short")
    public void should_use_globally_registered_custom_codec_with_simple_statements() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodec(jsonCodec, true) // global User <-> varchar codec
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(new SimpleStatement(insertQuery, notAJsonString, alice, bobAndCharlie));
        ResultSet rows = session.execute(new SimpleStatement(selectQuery, notAJsonString, alice));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_globally_registered_custom_codec_with_prepared_statements_1() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodec(jsonCodec, true) // global User <-> varchar codec
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind(notAJsonString, alice, bobAndCharlie));
        PreparedStatement ps = session.prepare(selectQuery);
        // this bind() method does not convey information about the java type of alice
        // so the registry will look for a codec accepting varchar <-> ANY
        // and will find jsonCodec because it is the first registered
        ResultSet rows = session.execute(ps.bind(notAJsonString, alice));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_globally_registered_custom_codec_with_prepared_statements_2() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodec(jsonCodec, true) // global User <-> varchar codec
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind()
                .setString(0, notAJsonString)
                .setObject(1, alice, User.class)
                .setList(2, bobAndCharlie)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setString(0, notAJsonString)
                // this setObject() method conveys information about the java type of alice
                // so the registry will look for a codec accepting varchar <-> User
                // and will find jsonCodec because it is the only matching one
                .setObject(1, alice, User.class)
        );
        Row row = rows.one();
        assertRow(row);
        rows = session.execute(ps.bind()
                .setString(0, notAJsonString)
                // here we lost information about the java type of alice
                // so the registry will look for a codec accepting varchar <-> ANY
                // and will find jsonCodec because it is the only matching one
                .setObject(1, alice)
        );
        row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_overriding_codec() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withOverridingCodec("test", "myTable", "c2", jsonCodec) // only test."myTable".c2 uses User <-> varchar codec
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        try {
            session.execute(new SimpleStatement(insertQuery, notAJsonString, alice, bobAndCharlie));
            fail("Cannot use overriding codecs with simple statements");
        } catch (Exception e) {
            //ok
        }
        try {
            session.execute(session.prepare(insertQuery).bind()
                    .setString(0, notAJsonString)
                    .setObject(1, alice) // no Java type information so this would use VarcharCodec, but jsonCodec is overriding
                    .setList(2, bobAndCharlie)
            );
            fail("Column c3 should not use overriding codec");
        } catch (Exception e) {
            //ok
        }
        session.execute(session.prepare(insertQuery).bind()
                .setString(0, notAJsonString)
                .setObject(1, alice) // no Java type information so this would use VarcharCodec, but jsonCodec is overriding
                .setList(2, Lists.newArrayList(bobJson, charlieJson))
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
            .setString(0, notAJsonString)
            .setObject(1, alice) // no Java type information so this would use VarcharCodec, but jsonCodec is overriding
        );
        Row row = rows.one();
        // getString requires a codec accepting varchar <-> String, so VarcharCodec is used
        assertThat(row.getString(0)).isEqualTo(notAJsonString);
        // getObject requires a codec accepting varchar <-> ANY;
        // the first codec that accepts that is jsonCodec, so it is used
        assertThat(row.getObject(1)).isEqualTo(alice);
        // getObject requires a codec accepting varchar <-> User;
        // jsonCodec is the only codec that accepts varchar <-> User, so it is used
        assertThat(row.getObject(1, User.class)).isEqualTo(alice);
        // we still can get the column as a string using VarcharCodec.instance behind the scenes
        try {
            // will find an overriding codec jsonCodec that does not accept varchar <-> String
            row.getString(1);
            fail("Should not deserialize c2 as a String because of overriding codec");
        } catch (CodecNotFoundException e) {
            // ok
        }
        try {
            // will not found codec for list<varchar> <-> List<User> because jsonCodec is not registered globally
            assertThat(row.getList(2, User.class)).containsExactly(bob, charlie);
            fail("Should not deserialize c3 as a List<User> because jsonCodec is not globally registered");
        } catch (CodecNotFoundException e) {
            // ok
        }
        // we still can get the column as a List<String>
        assertThat(row.getList(2, String.class)).containsExactly(bobJson, charlieJson);
    }

    private void assertRow(Row row) {
        // getString requires a codec accepting varchar <-> String, so VarcharCodec is used
        assertThat(row.getString(0)).isEqualTo(notAJsonString);
        // getObject requires a codec accepting varchar <-> ANY;
        // the first codec that accepts that is jsonCodec, so it is used
        assertThat(row.getObject(1)).isEqualTo(alice);
        // getObject requires a codec accepting varchar <-> User;
        // jsonCodec is the only codec that accepts varchar <-> User, so it is used
        assertThat(row.getObject(1, User.class)).isEqualTo(alice);
        // we still can get the column as a string using VarcharCodec.instance behind the scenes
        assertThat(row.getString(1)).isEqualTo(aliceJson);
        assertThat(row.getList(2, User.class)).containsExactly(bob, charlie);
        // we still can get the column as a List<String>
        assertThat(row.getList(2, String.class)).containsExactly(bobJson, charlieJson);
        try {
            // getObject requires a codec accepting varchar <-> ANY;
            // the first codec that accepts that is jsonCodec, so it is used
            // but the value is not JSON -> InvalidTypeException
            row.getObject(0);
            fail("This should not have worked");
        } catch (InvalidTypeException e) {
            // ok
        }
    }

    private void createSession() {
        cluster.init();
        session = cluster.connect();
        for (String statement : schema)
            session.execute(statement);
    }
}
