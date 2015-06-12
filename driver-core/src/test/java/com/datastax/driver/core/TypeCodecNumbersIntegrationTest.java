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

public class TypeCodecNumbersIntegrationTest {

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
    public void should_use_defaut_codecs_with_simple_statements() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(new SimpleStatement(insertQuery, n_int, n_bigint, n_float, n_double, n_varint, n_decimal));
        ResultSet rows = session.execute(new SimpleStatement(selectQuery, n_int, n_bigint));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_defaut_codecs_with_prepared_statements_1() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind(n_int, n_bigint, n_float, n_double, n_varint, n_decimal));
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind(n_int, n_bigint));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_default_codecs_with_prepared_statements_2() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind()
                .setInt(0, n_int)
                .setLong(1, n_bigint)
                .setFloat(2, n_float)
                .setDouble(3, n_double)
                .setVarint(4, n_varint)
                .setDecimal(5, n_decimal)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setInt(0, n_int)
                .setLong(1, n_bigint)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_default_codecs_with_prepared_statements_3() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind()
                .setObject(0, n_int)
                .setObject(1, n_bigint)
                .setObject(2, n_float)
                .setObject(3, n_double)
                .setObject(4, n_varint)
                .setObject(5, n_decimal)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setInt(0, n_int)
                .setLong(1, n_bigint)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_default_codecs_with_prepared_statements_4() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind()
                .setObject(0, n_int, Integer.class)
                .setObject(1, n_bigint, Long.class)
                .setObject(2, n_float, Float.class)
                .setObject(3, n_double, Double.class)
                .setObject(4, n_varint, BigInteger.class)
                .setObject(5, n_decimal, BigDecimal.class)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setInt(0, n_int)
                .setLong(1, n_bigint)
        );
        Row row = rows.one();
        assertRow(row);
    }

    private void assertRow(Row row) {
        assertThat(row.getInt(0)).isEqualTo(n_int);
        assertThat(row.getLong(1)).isEqualTo(n_bigint);
        assertThat(row.getFloat(2)).isEqualTo(n_float);
        assertThat(row.getDouble(3)).isEqualTo(n_double);
        assertThat(row.getVarint(4)).isEqualTo(n_varint);
        assertThat(row.getDecimal(5)).isEqualTo(n_decimal);
        // with getObject
        assertThat(row.getObject(0)).isEqualTo(n_int);
        assertThat(row.getObject(1)).isEqualTo(n_bigint);
        assertThat(row.getObject(2)).isEqualTo(n_float);
        assertThat(row.getObject(3)).isEqualTo(n_double);
        assertThat(row.getObject(4)).isEqualTo(n_varint);
        assertThat(row.getObject(5)).isEqualTo(n_decimal);
        // with getObject + type
        assertThat(row.getObject(0, Integer.class)).isEqualTo(n_int);
        assertThat(row.getObject(1, Long.class)).isEqualTo(n_bigint);
        assertThat(row.getObject(2, Float.class)).isEqualTo(n_float);
        assertThat(row.getObject(3, Double.class)).isEqualTo(n_double);
        assertThat(row.getObject(4, BigInteger.class)).isEqualTo(n_varint);
        assertThat(row.getObject(5, BigDecimal.class)).isEqualTo(n_decimal);
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
