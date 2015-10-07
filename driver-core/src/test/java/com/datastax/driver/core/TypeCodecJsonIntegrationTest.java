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
import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.TypeCodecTest.User;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class TypeCodecJsonIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final JsonCodec<User> jsonCodec = new JsonCodec<User>(User.class);

    private static final User alice = new User(1, "Alice");
    private static final User bob = new User(2, "Bob");
    private static final User charlie = new User(3, "Charlie");

    private static final String bobJson = "{\"id\":2,\"name\":\"Bob\"}";
    private static final String charlieJson = "{\"id\":3,\"name\":\"Charlie\"}";
    private static final String aliceJson = "{\"id\":1,\"name\":\"Alice\"}";

    private static final ArrayList<User> bobAndCharlie = Lists.newArrayList(bob, charlie);

    private static final String insertQuery = "INSERT INTO \"myTable\" (c1, c2, c3) VALUES (?, ?, ?)";
    private static final String selectQuery = "SELECT c1, c2, c3 FROM \"myTable\" WHERE c1 = ? and c2 = ?";

    private static final String notAJsonString = "this text is not json";

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE \"myTable\" (c1 text, c2 text, c3 list<text>, PRIMARY KEY (c1, c2))"
        );
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withCodecRegistry(
            new CodecRegistry().register(jsonCodec) // global User <-> varchar codec
        );
    }

    @Test(groups = "short")
    @CassandraVersion(major=2.0)
    public void should_use_custom_codec_with_simple_statements() {
        session.execute(insertQuery, notAJsonString, alice, bobAndCharlie);
        ResultSet rows = session.execute(selectQuery, notAJsonString, alice);
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codec_with_built_statements_1() {
        BuiltStatement insertStmt = new QueryBuilder(cluster).insertInto("\"myTable\"")
            .value("c1", bindMarker())
            .value("c2", bindMarker())
            .value("c3", bindMarker());
        BuiltStatement selectStmt = new QueryBuilder(cluster).select("c1", "c2", "c3")
            .from("\"myTable\"")
            .where(eq("c1", bindMarker()))
            .and(eq("c2", bindMarker()));
        session.execute(session.prepare(insertStmt).bind(notAJsonString, alice, bobAndCharlie));
        ResultSet rows = session.execute(session.prepare(selectStmt).bind(notAJsonString, alice));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codec_with_built_statements_2() {
        BuiltStatement insertStmt = new QueryBuilder(cluster).insertInto("\"myTable\"")
            .value("c1", notAJsonString)
            .value("c2", alice)
            .value("c3", bobAndCharlie);
        BuiltStatement selectStmt =
            new QueryBuilder(cluster).select("c1", "c2", "c3")
                .from("\"myTable\"")
                .where(eq("c1", notAJsonString))
                .and(eq("c2", alice));
        session.execute(insertStmt);
        ResultSet rows = session.execute(selectStmt);
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codec_with_prepared_statements_1() {
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
    public void should_use_custom_codec_with_prepared_statements_2() {
        session.execute(session.prepare(insertQuery).bind()
            .setString(0, notAJsonString)
            .set(1, alice, User.class)
            .setList(2, bobAndCharlie)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
            .setString(0, notAJsonString)
            // this set() method conveys information about the java type of alice
            // so the registry will look for a codec accepting varchar <-> User
            // and will find jsonCodec because it is the only matching one
            .set(1, alice, User.class)
        );
        Row row = rows.one();
        assertRow(row);
    }

    private void assertRow(Row row) {
        // getString requires a codec accepting varchar <-> String, so VarcharCodec is used
        assertThat(row.getString(0)).isEqualTo(notAJsonString);
        // getObject requires a codec accepting varchar <-> ANY;
        // the first codec that accepts that is jsonCodec, so it is used
        assertThat(row.getObject(1)).isEqualTo(aliceJson);
        // getObject uses the default codec VarcharCodec
        assertThat(row.get(1, User.class)).isEqualTo(alice);
        // we still can get the column as a string using VarcharCodec.instance behind the scenes
        assertThat(row.getString(1)).isEqualTo(aliceJson);
        assertThat(row.getList(2, User.class)).containsExactly(bob, charlie);
        // we still can get the column as a List<String>
        assertThat(row.getList(2, String.class)).containsExactly(bobJson, charlieJson);
    }
}
