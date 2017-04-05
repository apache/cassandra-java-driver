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

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.core.utils.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that nested collections are properly encoded,
 * even if some inner type requires a custom codec.
 */
@CassandraVersion("2.1.0")
public class TypeCodecNestedCollectionsIntegrationTest extends CCMTestsSupport {

    private final String insertQuery = "INSERT INTO \"myTable\" (pk, v) VALUES (?, ?)";

    private final String selectQuery = "SELECT pk, v FROM \"myTable\" WHERE pk = ?";

    private BuiltStatement insertStmt;
    private BuiltStatement selectStmt;

    private int pk = 42;
    private List<Set<Map<MyInt, String>>> v;

    // @formatter:off
    private TypeToken<List<Set<Map<MyInt, String>>>> listType = new TypeToken<List<Set<Map<MyInt, String>>>>() {};
    private TypeToken<Set<Map<MyInt, String>>> elementsType = new TypeToken<Set<Map<MyInt, String>>>() {};
    // @formatter:on


    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE IF NOT EXISTS \"myTable\" ("
                        + "pk int PRIMARY KEY, "
                        + "v frozen<list<frozen<set<frozen<map<int,text>>>>>>"
                        + ")"
        );
    }

    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(
                new CodecRegistry().register(new MyIntCodec()) // global User <-> varchar codec
        );
    }

    @BeforeClass(groups = "short")
    public void setupData() {
        Map<MyInt, String> map = ImmutableMap.of(new MyInt(42), "foo", new MyInt(43), "bar");
        Set<Map<MyInt, String>> set = new HashSet<Map<MyInt, String>>();
        set.add(map);
        v = new ArrayList<Set<Map<MyInt, String>>>();
        v.add(set);
    }

    @BeforeMethod(groups = "short")
    public void createBuiltStatements() throws Exception {
        insertStmt = insertInto("\"myTable\"")
                .value("pk", bindMarker())
                .value("v", bindMarker());
        selectStmt = select("pk", "v")
                .from("\"myTable\"")
                .where(eq("pk", bindMarker()));
    }

    @Test(groups = "short")
    public void should_work_with_simple_statements() {
        session().execute(insertQuery, pk, v);
        ResultSet rows = session().execute(selectQuery, pk);
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_work_with_prepared_statements_1() {
        session().execute(session().prepare(insertQuery).bind(pk, v));
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind(pk));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_work_with_prepared_statements_2() {
        session().execute(session().prepare(insertQuery).bind()
                .setInt(0, pk)
                .setList(1, v, elementsType) // variant with element type explicitly set
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, pk)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_work_with_prepared_statements_3() {
        session().execute(session().prepare(insertQuery).bind()
                        .setInt(0, pk)
                        .set(1, v, listType)
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, pk)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_work_with_built_statements() {
        session().execute(session().prepare(insertStmt).bind()
                        .setInt(0, pk)
                        .set(1, v, listType)
        );
        PreparedStatement ps = session().prepare(selectStmt);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, pk)
        );
        Row row = rows.one();
        assertRow(row);
    }

    private void assertRow(Row row) {
        assertThat(row.getList(1, elementsType)).isEqualTo(v);
        assertThat(row.get(1, listType)).isEqualTo(v);
    }

    private class MyInt {

        private final int i;

        private MyInt(int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MyInt myInt = (MyInt) o;
            return MoreObjects.equal(i, myInt.i);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(i);
        }
    }

    private class MyIntCodec extends TypeCodec<MyInt> {

        MyIntCodec() {
            super(DataType.cint(), MyInt.class);
        }

        @Override
        public ByteBuffer serialize(MyInt value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return TypeCodec.cint().serialize(value.i, protocolVersion);
        }

        @Override
        public MyInt deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return new MyInt(TypeCodec.cint().deserialize(bytes, protocolVersion));
        }

        @Override
        public MyInt parse(String value) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public String format(MyInt value) throws InvalidTypeException {
            return null; // not tested
        }
    }
}
