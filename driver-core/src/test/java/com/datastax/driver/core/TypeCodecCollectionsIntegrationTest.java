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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;

public class TypeCodecCollectionsIntegrationTest {

    private CCMBridge ccm;
    private Cluster cluster;
    private Session session;

    private List<String> schema = newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE test",
        "CREATE TABLE IF NOT EXISTS \"myTable2\" ("
            + "c_int int PRIMARY KEY, "
            + "l_int list<int>, "
            + "l_bigint list<bigint>, "
            + "s_float set<float>, "
            + "s_double set<double>, "
            + "m_varint map<int,varint>, "
            + "m_decimal map<int,decimal>"
            + ")"
    );

    private final String insertQuery = "INSERT INTO \"myTable2\" (c_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal) VALUES (?, ?, ?, ?, ?, ?, ?)";

    private final String selectQuery = "SELECT c_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal FROM \"myTable2\" WHERE c_int = ?";

    private int n_int = 42;
    private List<Integer> l_int = newArrayList(42, 43);
    private List<Long> l_bigint = newArrayList(42l, 43l);
    private Set<Float> s_float = newHashSet(42.42f, 43.43f);
    private Set<Double> s_double = newHashSet(42.42d, 43.43d);
    private Map<Integer, BigInteger> m_varint = ImmutableMap.of(42, new BigInteger("424242"), 43, new BigInteger("434343"));
    private Map<Integer, BigDecimal> m_decimal = ImmutableMap.of(42, new BigDecimal("424242.42"), 43, new BigDecimal("434343.43"));

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
    public void should_use_collection_codecs_with_simple_statements() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(new SimpleStatement(insertQuery, n_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal));
        ResultSet rows = session.execute(new SimpleStatement(selectQuery, n_int));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_1() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(
                CodecRegistry.builder()
                    .withDefaultCodecs()
                    .build()
            )
            .build();
        createSession();
        session.execute(session.prepare(insertQuery).bind(n_int, l_int, l_bigint, s_float, s_double, m_varint, m_decimal));
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind(n_int));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_2() {
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
                .setList(1, l_int)
                .setList(2, l_bigint)
                .setSet(3, s_float)
                .setSet(4, s_double)
                .setMap(5, m_varint)
                .setMap(6, m_decimal)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setInt(0, n_int)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_3() {
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
                .setObject(1, l_int)
                .setObject(2, l_bigint)
                .setObject(3, s_float)
                .setObject(4, s_double)
                .setObject(5, m_varint)
                .setObject(6, m_decimal)
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
                .setInt(0, n_int)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_collection_codecs_with_prepared_statements_4() {
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
                .setObject(1, l_int, listOf(Integer.class))
                .setObject(2, l_bigint, listOf(Long.class))
                .setObject(3, s_float, setOf(Float.class))
                .setObject(4, s_double, setOf(Double.class))
                .setObject(5, m_varint, mapOf(Integer.class, BigInteger.class))
                .setObject(6, m_decimal, mapOf(Integer.class, BigDecimal.class))
        );
        PreparedStatement ps = session.prepare(selectQuery);
        ResultSet rows = session.execute(ps.bind()
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
        // with getObject + type
        assertThat(row.getObject(1, listOf(Integer.class))).isEqualTo(l_int);
        assertThat(row.getObject(2, listOf(Long.class))).isEqualTo(l_bigint);
        assertThat(row.getObject(3, setOf(Float.class))).isEqualTo(s_float);
        assertThat(row.getObject(4, setOf(Double.class))).isEqualTo(s_double);
        assertThat(row.getObject(5, mapOf(Integer.class, BigInteger.class))).isEqualTo(m_varint);
        assertThat(row.getObject(6, mapOf(Integer.class, BigDecimal.class))).isEqualTo(m_decimal);
        // with getObject
        assertThat(row.getObject(1)).isEqualTo(l_int);
        assertThat(row.getObject(2)).isEqualTo(l_bigint);
        assertThat(row.getObject(3)).isEqualTo(s_float);
        assertThat(row.getObject(4)).isEqualTo(s_double);
        assertThat(row.getObject(5)).isEqualTo(m_varint);
        assertThat(row.getObject(6)).isEqualTo(m_decimal);
    }

    private void createSession() {
        cluster.init();
        session = cluster.connect();
        for (String statement : schema)
            session.execute(statement);
    }
}
