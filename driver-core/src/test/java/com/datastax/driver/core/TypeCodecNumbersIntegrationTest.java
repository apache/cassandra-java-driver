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

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeCodecNumbersIntegrationTest extends CCMTestsSupport {

    private final String insertQuery = "INSERT INTO \"myTable\" (c_int, c_bigint, c_float, c_double, c_varint, c_decimal) VALUES (?, ?, ?, ?, ?, ?)";

    private final String selectQuery = "SELECT c_int, c_bigint, c_float, c_double, c_varint, c_decimal FROM \"myTable\" WHERE c_int = ? and c_bigint = ?";

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

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_use_defaut_codecs_with_simple_statements() {
        session().execute(insertQuery, n_int, n_bigint, n_float, n_double, n_varint, n_decimal);
        ResultSet rows = session().execute(selectQuery, n_int, n_bigint);
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_defaut_codecs_with_prepared_statements_1() {
        session().execute(session().prepare(insertQuery).bind(n_int, n_bigint, n_float, n_double, n_varint, n_decimal));
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind(n_int, n_bigint));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_default_codecs_with_prepared_statements_2() {
        session().execute(session().prepare(insertQuery).bind()
                        .setInt(0, n_int)
                        .setLong(1, n_bigint)
                        .setFloat(2, n_float)
                        .setDouble(3, n_double)
                        .setVarint(4, n_varint)
                        .setDecimal(5, n_decimal)
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, n_int)
                        .setLong(1, n_bigint)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_default_codecs_with_prepared_statements_3() {
        session().execute(session().prepare(insertQuery).bind()
                        .set(0, n_int, Integer.class)
                        .set(1, n_bigint, Long.class)
                        .set(2, n_float, Float.class)
                        .set(3, n_double, Double.class)
                        .set(4, n_varint, BigInteger.class)
                        .set(5, n_decimal, BigDecimal.class)
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
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
        // with get + type
        assertThat(row.get(0, Integer.class)).isEqualTo(n_int);
        assertThat(row.get(1, Long.class)).isEqualTo(n_bigint);
        assertThat(row.get(2, Float.class)).isEqualTo(n_float);
        assertThat(row.get(3, Double.class)).isEqualTo(n_double);
        assertThat(row.get(4, BigInteger.class)).isEqualTo(n_varint);
        assertThat(row.get(5, BigDecimal.class)).isEqualTo(n_decimal);
    }
}
