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

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.0.0")
public class TypeCodecCollectionsIntegrationTest extends CCMTestsSupport {

    private final String insertQuery = "INSERT INTO \"myTable2\" (c_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal) VALUES (?, ?, ?, ?, ?, ?, ?)";

    private final String selectQuery = "SELECT c_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal FROM \"myTable2\" WHERE c_int = ?";

    private BuiltStatement insertStmt;
    private BuiltStatement selectStmt;

    private int n_int = 42;
    private List<Integer> l_int = newArrayList(42, 43);
    private List<Long> l_bigint = newArrayList(42L, 43L);
    private Set<Float> s_float = newHashSet(42.42f, 43.43f);
    private Set<Double> s_double = newHashSet(42.42d, 43.43d);
    private Map<Integer, BigInteger> m_varint = ImmutableMap.of(42, new BigInteger("424242"), 43, new BigInteger("434343"));
    private Map<Integer, BigDecimal> m_decimal = ImmutableMap.of(42, new BigDecimal("424242.42"), 43, new BigDecimal("434343.43"));

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE IF NOT EXISTS \"myTable2\" ("
                        + "c_int int PRIMARY KEY, "
                        + "l_int list<int>, "
                        + "l_bigint list<bigint>, "
                        + "s_float set<float>, "
                        + "s_double set<double>, "
                        + "m_varint map<int,varint>, "
                        + "m_decimal map<int,decimal>"
                        + ")");
    }

    @BeforeMethod(groups = "short")
    public void createBuiltStatements() throws Exception {
        insertStmt = insertInto("\"myTable2\"")
                .value("c_int", bindMarker())
                .value("l_int", bindMarker())
                .value("l_bigint", bindMarker())
                .value("s_float", bindMarker())
                .value("s_double", bindMarker())
                .value("m_varint", bindMarker())
                .value("m_decimal", bindMarker());
        selectStmt = select("c_int", "l_int", "l_bigint", "s_float", "s_double", "m_varint", "m_decimal")
                .from("\"myTable2\"")
                .where(eq("c_int", bindMarker()));
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_simple_statements() {
        session().execute(insertQuery, n_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal);
        ResultSet rows = session().execute(selectQuery, n_int);
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_1() {
        session().execute(session().prepare(insertQuery).bind(n_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal));
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind(n_int));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_2() {
        session().execute(session().prepare(insertQuery).bind()
                .setInt(0, n_int)
                .setList(1, l_int)
                .setList(2, l_bigint, Long.class) // variant with element type explicitly set
                .setSet(3, s_float)
                .setSet(4, s_double, TypeToken.of(Double.class))  // variant with element type explicitly set
                .setMap(5, m_varint)
                .setMap(6, m_decimal, Integer.class, BigDecimal.class)  // variant with element type explicitly set
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, n_int)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_3() {
        session().execute(session().prepare(insertQuery).bind()
                        .setInt(0, n_int)
                        .set(1, l_int, TypeTokens.listOf(Integer.class))
                        .set(2, l_bigint, TypeTokens.listOf(Long.class))
                        .set(3, s_float, TypeTokens.setOf(Float.class))
                        .set(4, s_double, TypeTokens.setOf(Double.class))
                        .set(5, m_varint, TypeTokens.mapOf(Integer.class, BigInteger.class))
                        .set(6, m_decimal, TypeTokens.mapOf(Integer.class, BigDecimal.class))
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, n_int)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_built_statements() {
        session().execute(session().prepare(insertStmt).bind()
                        .setInt(0, n_int)
                        .set(1, l_int, TypeTokens.listOf(Integer.class))
                        .set(2, l_bigint, TypeTokens.listOf(Long.class))
                        .set(3, s_float, TypeTokens.setOf(Float.class))
                        .set(4, s_double, TypeTokens.setOf(Double.class))
                        .set(5, m_varint, TypeTokens.mapOf(Integer.class, BigInteger.class))
                        .set(6, m_decimal, TypeTokens.mapOf(Integer.class, BigDecimal.class))
        );
        PreparedStatement ps = session().prepare(selectStmt);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, n_int)
        );
        Row row = rows.one();
        assertRow(row);
    }

    private void assertRow(Row row) {
        assertThat(row.getInt(0)).isEqualTo(n_int);
        assertThat(row.getList(1, Integer.class)).isEqualTo(l_int);
        assertThat(row.getList(2, Long.class)).isEqualTo(l_bigint);
        assertThat(row.getSet(3, Float.class)).isEqualTo(s_float);
        assertThat(row.getSet(4, Double.class)).isEqualTo(s_double);
        assertThat(row.getMap(5, Integer.class, BigInteger.class)).isEqualTo(m_varint);
        assertThat(row.getMap(6, Integer.class, BigDecimal.class)).isEqualTo(m_decimal);
        // with get + type
        assertThat(row.get(1, TypeTokens.listOf(Integer.class))).isEqualTo(l_int);
        assertThat(row.get(2, TypeTokens.listOf(Long.class))).isEqualTo(l_bigint);
        assertThat(row.get(3, TypeTokens.setOf(Float.class))).isEqualTo(s_float);
        assertThat(row.get(4, TypeTokens.setOf(Double.class))).isEqualTo(s_double);
        assertThat(row.get(5, TypeTokens.mapOf(Integer.class, BigInteger.class))).isEqualTo(m_varint);
        assertThat(row.get(6, TypeTokens.mapOf(Integer.class, BigDecimal.class))).isEqualTo(m_decimal);
        // with getObject
        assertThat(row.getObject(1)).isEqualTo(l_int);
        assertThat(row.getObject(2)).isEqualTo(l_bigint);
        assertThat(row.getObject(3)).isEqualTo(s_float);
        assertThat(row.getObject(4)).isEqualTo(s_double);
        assertThat(row.getObject(5)).isEqualTo(m_varint);
        assertThat(row.getObject(6)).isEqualTo(m_decimal);
    }
}
