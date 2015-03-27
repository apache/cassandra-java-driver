/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.utils.CassandraVersion;

/**
 * The goal of this test is to cover the serialization and deserialization of datatypes.
 *
 * It creates a table with a column of a given type, inserts a value and then tries to retrieve it.
 * There are 3 variants for the insert query: a raw string, a simple statement with a parameter
 * (protocol > v2 only) and a prepared statement.
 * This is repeated with a large number of datatypes.
 */
public class DataTypeIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {
    private static final Logger logger = LoggerFactory.getLogger(DataTypeIntegrationTest.class);

    List<TestTable> tables = allTables();
    VersionNumber cassandraVersion;

    enum StatementType {RAW_STRING, SIMPLE_WITH_PARAM, PREPARED}

    @Override
    protected Collection<String> getTableDefinitions() {
        Host host = cluster.getMetadata().getAllHosts().iterator().next();
        cassandraVersion = host.getCassandraVersion().nextStable();

        List<String> statements = Lists.newArrayList();
        for (TestTable table : tables) {
            if (cassandraVersion.compareTo(table.minCassandraVersion) < 0)
                logger.debug("Skipping table because it uses a feature not supported by Cassandra {}: {}",
                    cassandraVersion, table.createStatement);
            else
                statements.add(table.createStatement);
        }

        return statements;
    }

    @Test(groups = "long")
    public void should_insert_and_retrieve_data_with_legacy_statements() {
        should_insert_and_retrieve_data(StatementType.RAW_STRING);
    }

    @Test(groups = "long")
    public void should_insert_and_retrieve_data_with_prepared_statements() {
        should_insert_and_retrieve_data(StatementType.PREPARED);
    }

    @Test(groups = "long")
    @CassandraVersion(major = 2.0, description = "Uses parameterized simple statements, which are only available with protocol v2")
    public void should_insert_and_retrieve_data_with_parameterized_simple_statements() {
        should_insert_and_retrieve_data(StatementType.SIMPLE_WITH_PARAM);
    }

    protected void should_insert_and_retrieve_data(StatementType statementType) {
        for (TestTable table : tables) {
            if (cassandraVersion.compareTo(table.minCassandraVersion) < 0)
                continue;

            switch (statementType) {
                case RAW_STRING:
                    session.execute(table.insertStatement.replace("?", table.testColumnType.format(table.sampleValue)));
                    break;
                case SIMPLE_WITH_PARAM:
                    session.execute(table.insertStatement, table.sampleValue);
                    break;
                case PREPARED:
                    PreparedStatement ps = session.prepare(table.insertStatement);
                    BoundStatement bs = ps.bind(table.sampleValue);
                    checkGetterReturnsBoundValue(bs, table);
                    session.execute(bs);
                    break;
            }

            Row row = session.execute(table.selectStatement).one();
            Object queriedValue = table.testColumnType.deserialize(row.getBytesUnsafe("v"));

            assertThat(queriedValue)
                .as("Test failure on %s statement with table:%n%s;%n" +
                        "insert statement:%n%s;%n",
                    statementType,
                    table.createStatement,
                    table.insertStatement)
                .isEqualTo(table.sampleValue);

            session.execute(table.truncateStatement);
        }
    }

    private void checkGetterReturnsBoundValue(BoundStatement bs, TestTable table) {
        Object getterResult = getBoundValue(bs, table.testColumnType);
        assertThat(getterResult).isEqualTo(table.sampleValue);

        // Ensure that bs.getObject() also returns the expected value.
        assertThat(bs.getObject(0)).isEqualTo(table.sampleValue);
    }

    /**
     * Abstracts information about a table (corresponding to a given column type).
     */
    static class TestTable {
        private static final AtomicInteger counter = new AtomicInteger();
        private String tableName = "date_type_test" + counter.incrementAndGet();

        final DataType testColumnType;
        final Object sampleValue;

        final String createStatement;
        final String insertStatement = String.format("INSERT INTO %s (k, v) VALUES (1, ?)", tableName);
        final String selectStatement = String.format("SELECT v FROM %s WHERE k = 1", tableName);
        final String truncateStatement = String.format("TRUNCATE %s", tableName);

        final VersionNumber minCassandraVersion;

        TestTable(DataType testColumnType, Object sampleValue, String minCassandraVersion) {
            this.testColumnType = testColumnType;
            this.sampleValue = sampleValue;
            this.minCassandraVersion = VersionNumber.parse(minCassandraVersion);

            this.createStatement = String.format("CREATE TABLE %s (k int PRIMARY KEY, v %s)", tableName, testColumnType);
        }
    }

    private static List<TestTable> allTables() {
        List<TestTable> tables = Lists.newArrayList();

        tables.addAll(tablesWithPrimitives());
        tables.addAll(tablesWithCollectionsOfPrimitives());
        tables.addAll(tablesWithMapsOfPrimitives());

        return ImmutableList.copyOf(tables);
    }

    private static List<TestTable> tablesWithPrimitives() {
        List<TestTable> tables = Lists.newArrayList();
        for (Map.Entry<DataType, Object> entry : PrimitiveTypeSamples.ALL.entrySet())
            tables.add(new TestTable(entry.getKey(), entry.getValue(), "1.2.0"));
        return tables;
    }

    private static List<TestTable> tablesWithCollectionsOfPrimitives() {
        List<TestTable> tables = Lists.newArrayList();
        for (Map.Entry<DataType, Object> entry : PrimitiveTypeSamples.ALL.entrySet()) {

            DataType elementType = entry.getKey();
            Object elementSample = entry.getValue();

            tables.add(new TestTable(DataType.list(elementType), Lists.newArrayList(elementSample, elementSample), "1.2.0"));
            tables.add(new TestTable(DataType.set(elementType), Sets.newHashSet(elementSample), "1.2.0"));
        }
        return tables;
    }

    private static List<TestTable> tablesWithMapsOfPrimitives() {
        List<TestTable> tables = Lists.newArrayList();
        for (Map.Entry<DataType, Object> keyEntry : PrimitiveTypeSamples.ALL.entrySet()) {
            DataType keyType = keyEntry.getKey();
            Object keySample = keyEntry.getValue();
            for (Map.Entry<DataType, Object> valueEntry : PrimitiveTypeSamples.ALL.entrySet()) {
                DataType valueType = valueEntry.getKey();
                Object valueSample = valueEntry.getValue();

                tables.add(new TestTable(DataType.map(keyType, valueType),
                    ImmutableMap.builder().put(keySample, valueSample).build(),
                    "1.2.0"));
            }
        }
        return tables;
    }

    private Object getBoundValue(BoundStatement bs, DataType dataType) {
        // This is kind of lame, but better than testing all getters manually
        switch (dataType.getName()) {
            case ASCII:
                return bs.getString(0);
            case BIGINT:
                return bs.getLong(0);
            case BLOB:
                return bs.getBytes(0);
            case BOOLEAN:
                return bs.getBool(0);
            case DECIMAL:
                return bs.getDecimal(0);
            case DOUBLE:
                return bs.getDouble(0);
            case FLOAT:
                return bs.getFloat(0);
            case INET:
                return bs.getInet(0);
            case INT:
                return bs.getInt(0);
            case TEXT:
            case VARCHAR:
                return bs.getString(0);
            case TIMESTAMP:
                return bs.getDate(0);
            case UUID:
            case TIMEUUID:
                return bs.getUUID(0);
            case VARINT:
                return bs.getVarint(0);
            case LIST:
                return bs.getList(0, dataType.getTypeArguments().get(0).asJavaClass());
            case SET:
                return bs.getSet(0, dataType.getTypeArguments().get(0).asJavaClass());
            case MAP:
                return bs.getMap(0, dataType.getTypeArguments().get(0).asJavaClass(), dataType.getTypeArguments().get(1).asJavaClass());
            case CUSTOM:
            case COUNTER:
            default:
                fail("Unexpected type in bound statement test: " + dataType);
                return null;
        }
    }
}
