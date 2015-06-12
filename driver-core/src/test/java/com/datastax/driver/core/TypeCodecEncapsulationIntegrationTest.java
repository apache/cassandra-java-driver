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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class TypeCodecEncapsulationIntegrationTest {

    private CCMBridge ccm;
    private Cluster cluster;
    private Session session;

    private List<String> schema = newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE test",
        "CREATE TABLE IF NOT EXISTS \"myTable\" ("
            + "c_int int, "
            + "c_bigint bigint, "
            + "c_float float, "
            + "c_double double, "
            + "c_varint varint, "
            + "c_decimal decimal, "
            + "PRIMARY KEY (c_int, c_bigint)"
            + ")"
    );

    private final String insertQuery = "INSERT INTO \"myTable\" (c_int, c_bigint, c_float, c_double, c_varint, c_decimal) VALUES (?, ?, ?, ?, ?, ?)";

    private final String selectQuery = "SELECT c_int, c_bigint, c_float, c_double, c_varint, c_decimal FROM \"myTable\" WHERE c_int = ? and c_bigint = ?";

    private int n_int = 42;
    private long n_bigint = 4242;
    private float n_float = 42.42f;
    private double n_double = 4242.42d;
    private BigInteger n_varint = new BigInteger("424242");
    private BigDecimal n_decimal = new BigDecimal("424242.42");

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
    public void should_use_custom_codecs_with_simple_statements() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodecs(
                        new NumberBoxCodec<Integer>(IntCodec.instance),
                        new NumberBoxCodec<Long>(BigintCodec.instance),
                        new NumberBoxCodec<Float>(FloatCodec.instance),
                        new NumberBoxCodec<Double>(DoubleCodec.instance),
                        new NumberBoxCodec<BigInteger>(VarintCodec.instance),
                        new NumberBoxCodec<BigDecimal>(DecimalCodec.instance)
                    )
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(new SimpleStatement(insertQuery,
            n_int,
            new NumberBox<Long>(n_bigint),
            new NumberBox<Float>(n_float),
            new NumberBox<Double>(n_double),
            new NumberBox<BigInteger>(n_varint),
            new NumberBox<BigDecimal>(n_decimal)));
        ResultSet rows = session.execute(new SimpleStatement(selectQuery, n_int, new NumberBox<Long>(n_bigint)));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_prepared_statements_1() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodecs(
                        new NumberBoxCodec<Integer>(IntCodec.instance),
                        new NumberBoxCodec<Long>(BigintCodec.instance),
                        new NumberBoxCodec<Float>(FloatCodec.instance),
                        new NumberBoxCodec<Double>(DoubleCodec.instance),
                        new NumberBoxCodec<BigInteger>(VarintCodec.instance),
                        new NumberBoxCodec<BigDecimal>(DecimalCodec.instance)
                    )
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind(
            n_int,
            new NumberBox<Long>(n_bigint),
            new NumberBox<Float>(n_float),
            new NumberBox<Double>(n_double),
            new NumberBox<BigInteger>(n_varint),
            new NumberBox<BigDecimal>(n_decimal)));
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind(n_int, new NumberBox<Long>(n_bigint)));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_prepared_statements_2() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodecs(
                        new NumberBoxCodec<Integer>(IntCodec.instance),
                        new NumberBoxCodec<Long>(BigintCodec.instance),
                        new NumberBoxCodec<Float>(FloatCodec.instance),
                        new NumberBoxCodec<Double>(DoubleCodec.instance),
                        new NumberBoxCodec<BigInteger>(VarintCodec.instance),
                        new NumberBoxCodec<BigDecimal>(DecimalCodec.instance)
                    )
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind()
                .setObject(0, new NumberBox<Integer>(n_int))
                .setObject(1, new NumberBox<Long>(n_bigint))
                .setObject(2, new NumberBox<Float>(n_float))
                .setObject(3, new NumberBox<Double>(n_double))
                .setObject(4, new NumberBox<BigInteger>(n_varint))
                .setObject(5, new NumberBox<BigDecimal>(n_decimal))
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setObject(0, new NumberBox<Integer>(n_int))
                .setObject(1, new NumberBox<Long>(n_bigint))
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_prepared_statements_3() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withCodecs(
                        new NumberBoxCodec<Integer>(IntCodec.instance),
                        new NumberBoxCodec<Long>(BigintCodec.instance),
                        new NumberBoxCodec<Float>(FloatCodec.instance),
                        new NumberBoxCodec<Double>(DoubleCodec.instance),
                        new NumberBoxCodec<BigInteger>(VarintCodec.instance),
                        new NumberBoxCodec<BigDecimal>(DecimalCodec.instance)
                    )
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind()
                .setObject(0, new NumberBox<Integer>(n_int), new TypeToken<NumberBox<Integer>>(){})
                .setObject(1, new NumberBox<Long>(n_bigint), new TypeToken<NumberBox<Long>>(){})
                .setObject(2, new NumberBox<Float>(n_float), new TypeToken<NumberBox<Float>>(){})
                .setObject(3, new NumberBox<Double>(n_double), new TypeToken<NumberBox<Double>>(){})
                .setObject(4, new NumberBox<BigInteger>(n_varint), new TypeToken<NumberBox<BigInteger>>(){})
                .setObject(5, new NumberBox<BigDecimal>(n_decimal), new TypeToken<NumberBox<BigDecimal>>(){})
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setObject(0, new NumberBox<Integer>(n_int), new TypeToken<NumberBox<Integer>>(){})
                .setObject(1, new NumberBox<Long>(n_bigint), new TypeToken<NumberBox<Long>>(){})
        );
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
        // with getObject, the first matching codec for the CQL type is used
        // here it will be the NumberBox codecs because they were registered
        // first
        assertThat(row.getObject(0)).isEqualTo(new NumberBox<Integer>(n_int));
        assertThat(row.getObject(1)).isEqualTo(new NumberBox<Long>(n_bigint));
        assertThat(row.getObject(2)).isEqualTo(new NumberBox<Float>(n_float));
        assertThat(row.getObject(3)).isEqualTo(new NumberBox<Double>(n_double));
        assertThat(row.getObject(4)).isEqualTo(new NumberBox<BigInteger>(n_varint));
        assertThat(row.getObject(5)).isEqualTo(new NumberBox<BigDecimal>(n_decimal));
        // with getObject + type
        // we go back to the default codecs
        assertThat(row.getObject(0, Integer.class)).isEqualTo(n_int);
        assertThat(row.getObject(1, Long.class)).isEqualTo(n_bigint);
        assertThat(row.getObject(2, Float.class)).isEqualTo(n_float);
        assertThat(row.getObject(3, Double.class)).isEqualTo(n_double);
        assertThat(row.getObject(4, BigInteger.class)).isEqualTo(n_varint);
        assertThat(row.getObject(5, BigDecimal.class)).isEqualTo(n_decimal);
        // with getObject + type, but enforcing NumberBox types
        // we get the NumberBox codecs instead
        assertThat(row.getObject(0, new TypeToken<NumberBox<Integer>>(){})).isEqualTo(new NumberBox<Integer>(n_int));
        assertThat(row.getObject(1, new TypeToken<NumberBox<Long>>(){})).isEqualTo(new NumberBox<Long>(n_bigint));
        assertThat(row.getObject(2, new TypeToken<NumberBox<Float>>(){})).isEqualTo(new NumberBox<Float>(n_float));
        assertThat(row.getObject(3, new TypeToken<NumberBox<Double>>(){})).isEqualTo(new NumberBox<Double>(n_double));
        assertThat(row.getObject(4, new TypeToken<NumberBox<BigInteger>>(){})).isEqualTo(new NumberBox<BigInteger>(n_varint));
        assertThat(row.getObject(5, new TypeToken<NumberBox<BigDecimal>>(){})).isEqualTo(new NumberBox<BigDecimal>(n_decimal));
    }

    private void createSession() {
        cluster.init();
        session = cluster.connect();
        for (String statement : schema)
            session.execute(statement);
    }

    private class NumberBoxCodec<T extends Number> extends TypeCodec<NumberBox<T>> {

        private final TypeCodec<T> numberCodec;

        protected NumberBoxCodec(TypeCodec<T> numberCodec) {
            super(numberCodec.getCqlType(), new TypeToken<NumberBox<T>>(){}.where(new TypeParameter<T>(){}, numberCodec.getJavaType()));
            this.numberCodec = numberCodec;
        }

        public boolean accepts(Object value) {
            return value instanceof NumberBox && numberCodec.accepts(((NumberBox)value).getNumber());
        }

        @Override
        public ByteBuffer serialize(NumberBox<T> value) throws InvalidTypeException {
            return numberCodec.serialize(value.getNumber());
        }

        @Override
        public NumberBox<T> deserialize(ByteBuffer bytes) throws InvalidTypeException {
            return new NumberBox<T>(numberCodec.deserialize(bytes));
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
            NumberBox numberBox = (NumberBox)o;
            return Objects.equal(number, numberBox.number);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(number);
        }
    }
}
