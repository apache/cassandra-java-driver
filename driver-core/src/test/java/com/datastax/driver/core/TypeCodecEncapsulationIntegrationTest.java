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
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

public class TypeCodecEncapsulationIntegrationTest extends CCMTestsSupport {

    // @formatter:off
    private static final TypeToken<NumberBox<Integer>> NUMBERBOX_OF_INTEGER_TOKEN = new TypeToken<NumberBox<Integer>>() {};
    private static final TypeToken<NumberBox<Long>> NUMBERBOX_OF_LONG_TOKEN = new TypeToken<NumberBox<Long>>() {};
    private static final TypeToken<NumberBox<Float>> NUMBERBOX_OF_FLOAT_TOKEN = new TypeToken<NumberBox<Float>>() {};
    private static final TypeToken<NumberBox<Double>> NUMBERBOX_OF_DOUBLE_TOKEN = new TypeToken<NumberBox<Double>>() {};
    private static final TypeToken<NumberBox<BigInteger>> NUMBERBOX_OF_BIGINTEGER_TOKEN = new TypeToken<NumberBox<BigInteger>>() {};
    private static final TypeToken<NumberBox<BigDecimal>> NUMBERBOX_OF_BIGDECIMAL_TOKEN = new TypeToken<NumberBox<BigDecimal>>() {};
    // @formatter:on

    private final String insertQuery = "INSERT INTO \"myTable\" (c_int, c_bigint, c_float, c_double, c_varint, c_decimal) VALUES (?, ?, ?, ?, ?, ?)";

    private final String selectQuery = "SELECT c_int, c_bigint, c_float, c_double, c_varint, c_decimal FROM \"myTable\" WHERE c_int = ? and c_bigint = ?";

    private BuiltStatement insertStmt;
    private BuiltStatement selectStmt;

    private int n_int = 42;
    private long n_bigint = 4242;
    private float n_float = 42.42f;
    private double n_double = 4242.42d;
    private BigInteger n_varint = new BigInteger("424242");
    private BigDecimal n_decimal = new BigDecimal("424242.42");

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE \"myTable\" ("
                        + "c_int int, "
                        + "c_bigint bigint, "
                        + "c_float float, "
                        + "c_double double, "
                        + "c_varint varint, "
                        + "c_decimal decimal, "
                        + "PRIMARY KEY (c_int, c_bigint)"
                        + ")"
        );
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(
                new CodecRegistry()
                        .register(
                                new NumberBoxCodec<Integer>(TypeCodec.cint()),
                                new NumberBoxCodec<Long>(TypeCodec.bigint()),
                                new NumberBoxCodec<Float>(TypeCodec.cfloat()),
                                new NumberBoxCodec<Double>(TypeCodec.cdouble()),
                                new NumberBoxCodec<BigInteger>(TypeCodec.varint()),
                                new NumberBoxCodec<BigDecimal>(TypeCodec.decimal())
                        )
        );
    }

    @BeforeMethod(groups = "short")
    public void createBuiltStatements() throws Exception {
        insertStmt = insertInto("\"myTable\"")
                .value("c_int", bindMarker())
                .value("c_bigint", bindMarker())
                .value("c_float", bindMarker())
                .value("c_double", bindMarker())
                .value("c_varint", bindMarker())
                .value("c_decimal", bindMarker());
        selectStmt = select("c_int", "c_bigint", "c_float", "c_double", "c_varint", "c_decimal")
                .from("\"myTable\"")
                .where(eq("c_int", bindMarker()))
                .and(eq("c_bigint", bindMarker()));
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_use_custom_codecs_with_simple_statements() {
        session().execute(insertQuery,
                n_int,
                new NumberBox<Long>(n_bigint),
                new NumberBox<Float>(n_float),
                new NumberBox<Double>(n_double),
                new NumberBox<BigInteger>(n_varint),
                new NumberBox<BigDecimal>(n_decimal));
        ResultSet rows = session().execute(selectQuery, n_int, new NumberBox<Long>(n_bigint));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_prepared_statements_1() {
        session().execute(session().prepare(insertQuery).bind(
                n_int,
                new NumberBox<Long>(n_bigint),
                new NumberBox<Float>(n_float),
                new NumberBox<Double>(n_double),
                new NumberBox<BigInteger>(n_varint),
                new NumberBox<BigDecimal>(n_decimal)));
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind(n_int, new NumberBox<Long>(n_bigint)));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_prepared_statements_2() {
        session().execute(session().prepare(insertQuery).bind()
                        .set(0, new NumberBox<Integer>(n_int), NUMBERBOX_OF_INTEGER_TOKEN)
                        .set(1, new NumberBox<Long>(n_bigint), NUMBERBOX_OF_LONG_TOKEN)
                        .set(2, new NumberBox<Float>(n_float), NUMBERBOX_OF_FLOAT_TOKEN)
                        .set(3, new NumberBox<Double>(n_double), NUMBERBOX_OF_DOUBLE_TOKEN)
                        .set(4, new NumberBox<BigInteger>(n_varint), NUMBERBOX_OF_BIGINTEGER_TOKEN)
                        .set(5, new NumberBox<BigDecimal>(n_decimal), NUMBERBOX_OF_BIGDECIMAL_TOKEN)
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .set(0, new NumberBox<Integer>(n_int), NUMBERBOX_OF_INTEGER_TOKEN)
                        .set(1, new NumberBox<Long>(n_bigint), NUMBERBOX_OF_LONG_TOKEN)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_built_statements_1() {
        session().execute(session().prepare(insertStmt).bind(
                n_int,
                new NumberBox<Long>(n_bigint),
                new NumberBox<Float>(n_float),
                new NumberBox<Double>(n_double),
                new NumberBox<BigInteger>(n_varint),
                new NumberBox<BigDecimal>(n_decimal)));
        PreparedStatement ps = session().prepare(selectStmt);
        ResultSet rows = session().execute(ps.bind(n_int, new NumberBox<Long>(n_bigint)));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_built_statements_2() {
        session().execute(session().prepare(insertStmt).bind()
                .set(0, new NumberBox<Integer>(n_int), NUMBERBOX_OF_INTEGER_TOKEN)
                .set(1, new NumberBox<Long>(n_bigint), NUMBERBOX_OF_LONG_TOKEN)
                .set(2, new NumberBox<Float>(n_float), NUMBERBOX_OF_FLOAT_TOKEN)
                .set(3, new NumberBox<Double>(n_double), NUMBERBOX_OF_DOUBLE_TOKEN)
                .set(4, new NumberBox<BigInteger>(n_varint), NUMBERBOX_OF_BIGINTEGER_TOKEN)
                .set(5, new NumberBox<BigDecimal>(n_decimal), NUMBERBOX_OF_BIGDECIMAL_TOKEN));
        PreparedStatement ps = session().prepare(selectStmt);
        ResultSet rows = session().execute(ps.bind()
                .set(0, new NumberBox<Integer>(n_int), NUMBERBOX_OF_INTEGER_TOKEN)
                .set(1, new NumberBox<Long>(n_bigint), NUMBERBOX_OF_LONG_TOKEN));

        Row row = rows.one();
        assertRow(row);
    }

    private void assertRow(Row row) {
        // using getInt, etc: the default codecs are used
        // and values are deserialized the traditional way
        assertThat(row.getInt(0)).isEqualTo(n_int);
        assertThat(row.getLong(1)).isEqualTo(n_bigint);
        assertThat(row.getFloat(2)).isEqualTo(n_float);
        assertThat(row.getDouble(3)).isEqualTo(n_double);
        assertThat(row.getVarint(4)).isEqualTo(n_varint);
        assertThat(row.getDecimal(5)).isEqualTo(n_decimal);
        // with getObject, the first matching codec is the default one
        assertThat(row.getObject(0)).isEqualTo(n_int);
        assertThat(row.getObject(1)).isEqualTo(n_bigint);
        assertThat(row.getObject(2)).isEqualTo(n_float);
        assertThat(row.getObject(3)).isEqualTo(n_double);
        assertThat(row.getObject(4)).isEqualTo(n_varint);
        assertThat(row.getObject(5)).isEqualTo(n_decimal);
        // with get + type
        // we go back to the default codecs
        assertThat(row.get(0, Integer.class)).isEqualTo(n_int);
        assertThat(row.get(1, Long.class)).isEqualTo(n_bigint);
        assertThat(row.get(2, Float.class)).isEqualTo(n_float);
        assertThat(row.get(3, Double.class)).isEqualTo(n_double);
        assertThat(row.get(4, BigInteger.class)).isEqualTo(n_varint);
        assertThat(row.get(5, BigDecimal.class)).isEqualTo(n_decimal);
        // with get + type, but enforcing NumberBox types
        // we get the NumberBox codecs instead
        assertThat(row.get(0, NUMBERBOX_OF_INTEGER_TOKEN)).isEqualTo(new NumberBox<Integer>(n_int));
        assertThat(row.get(1, NUMBERBOX_OF_LONG_TOKEN)).isEqualTo(new NumberBox<Long>(n_bigint));
        assertThat(row.get(2, NUMBERBOX_OF_FLOAT_TOKEN)).isEqualTo(new NumberBox<Float>(n_float));
        assertThat(row.get(3, NUMBERBOX_OF_DOUBLE_TOKEN)).isEqualTo(new NumberBox<Double>(n_double));
        assertThat(row.get(4, NUMBERBOX_OF_BIGINTEGER_TOKEN)).isEqualTo(new NumberBox<BigInteger>(n_varint));
        assertThat(row.get(5, NUMBERBOX_OF_BIGDECIMAL_TOKEN)).isEqualTo(new NumberBox<BigDecimal>(n_decimal));
    }

    private class NumberBoxCodec<T extends Number> extends TypeCodec<NumberBox<T>> {

        private final TypeCodec<T> numberCodec;

        protected NumberBoxCodec(TypeCodec<T> numberCodec) {
            // @formatter:off
            super(numberCodec.getCqlType(),
                    new TypeToken<NumberBox<T>>() {}.where(new TypeParameter<T>() {}, numberCodec.getJavaType()));
            // @formatter:on
            this.numberCodec = numberCodec;
        }

        public boolean accepts(Object value) {
            return value instanceof NumberBox && numberCodec.accepts(((NumberBox) value).getNumber());
        }

        @Override
        public ByteBuffer serialize(NumberBox<T> value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return numberCodec.serialize(value.getNumber(), protocolVersion);
        }

        @Override
        public NumberBox<T> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return new NumberBox<T>(numberCodec.deserialize(bytes, protocolVersion));
        }

        @Override
        public NumberBox<T> parse(String value) throws InvalidTypeException {
            return new NumberBox<T>(numberCodec.parse(value));
        }

        @Override
        public String format(NumberBox<T> value) throws InvalidTypeException {
            return numberCodec.format(value.getNumber());
        }

    }

    private class NumberBox<T extends Number> {

        private final T number;

        private NumberBox(T number) {
            this.number = number;
        }

        public T getNumber() {
            return number;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            NumberBox numberBox = (NumberBox) o;
            return MoreObjects.equal(number, numberBox.number);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(number);
        }
    }
}
