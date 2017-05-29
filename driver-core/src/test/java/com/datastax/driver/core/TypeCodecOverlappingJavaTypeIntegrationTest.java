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
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test an edge case where the user register 2 codecs for the same Java type (here, String).
 * SimpleStatements become almost unusable in these cases, but we are adding tests
 * to check that at least it is possible to use prepared statements in these situations.
 */
public class TypeCodecOverlappingJavaTypeIntegrationTest extends CCMTestsSupport {

    private static final String insertQuery = "INSERT INTO \"myTable\" (c_int, l_int, c_text) VALUES (?, ?, ?)";

    private static final String selectQuery = "SELECT c_int, l_int, c_text FROM \"myTable\" WHERE c_int = ?";

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE \"myTable\" ("
                        + "c_int int PRIMARY KEY, "
                        + "l_int list<int>, "
                        + "c_text text "
                        + ")"
        );
    }

    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(
                new CodecRegistry().register(new IntToStringCodec())
        );
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_prepared_statements() {
        PreparedStatement ps = session().prepare(insertQuery);
        session().execute(
                ps.bind()
                        .setInt(0, 42)
                        .setList(1, newArrayList(42))
                        .setString(2, "42") // here we have the CQL type so VarcharCodec will be used even if IntToStringCodec accepts it
        );
        session().execute(
                ps.bind()
                        .setString(0, "42")
                        .setList(1, newArrayList("42"), String.class)
                        .setString(2, "42") // here we have the CQL type so VarcharCodec will be used even if IntToStringCodec accepts it
        );
        ps = session().prepare(selectQuery);
        assertRow(session().execute(ps.bind().setInt(0, 42)).one());
        assertRow(session().execute(ps.bind().setString(0, "42")).one());
    }

    private void assertRow(Row row) {
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getObject(0)).isEqualTo(42); // uses the default codec
        assertThat(row.get(0, Integer.class)).isEqualTo(42);
        assertThat(row.get(0, String.class)).isEqualTo("42");

        assertThat(row.getList(1, Integer.class)).isEqualTo(newArrayList(42));
        assertThat(row.getList(1, String.class)).isEqualTo(newArrayList("42"));
        assertThat(row.getObject(1)).isEqualTo(newArrayList(42)); // uses the default codec
        assertThat(row.get(1, TypeTokens.listOf(Integer.class))).isEqualTo(newArrayList(42));
        assertThat(row.get(1, TypeTokens.listOf(String.class))).isEqualTo(newArrayList("42"));
    }

    private class IntToStringCodec extends TypeCodec<String> {

        protected IntToStringCodec() {
            super(DataType.cint(), String.class);
        }

        @Override
        public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return TypeCodec.cint().serialize(value == null ? null : Integer.parseInt(value), protocolVersion);
        }

        @Override
        public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            Integer i = TypeCodec.cint().deserialize(bytes, protocolVersion);
            return i == null ? null : Integer.toString(i);
        }

        @Override
        public String parse(String value) throws InvalidTypeException {
            return value;
        }

        @Override
        public String format(String value) throws InvalidTypeException {
            return value;
        }

        @Override
        public boolean accepts(Object value) {
            return value instanceof String && ((String) value).matches("\\d+");
        }
    }
}
