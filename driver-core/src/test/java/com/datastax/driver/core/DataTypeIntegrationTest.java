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
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum();

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
                    session.execute(ps.bind(table.sampleValue));
                    break;
            }

            Row row = session.execute(table.selectStatement).one();
            Object queriedValue = table.testColumnType.deserialize(row.getBytesUnsafe("v"), protocolVersion);

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
        tables.addAll(tablesWithNestedCollections());
        tables.addAll(tablesWithRandomlyGeneratedNestedCollections());

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

    private static Collection<? extends TestTable> tablesWithNestedCollections() {
        List<TestTable> tables = Lists.newArrayList();

        // To avoid combinatorial explosion, only use int as the primitive type, and two levels of nesting.
        // This yields collections like list<frozen<map<int, int>>, map<frozen<set<int>>, frozen<list<int>>>, etc.

        // Types and samples for the inner collections like frozen<list<int>>
        Map<DataType, Object> childCollectionSamples = ImmutableMap.<DataType, Object>builder()
            .put(DataType.frozenList(DataType.cint()), Lists.newArrayList(1, 1))
            .put(DataType.frozenSet(DataType.cint()), Sets.newHashSet(1, 2))
            .put(DataType.frozenMap(DataType.cint(), DataType.cint()), ImmutableMap.<Integer, Integer>builder().put(1, 2).put(3, 4).build())
            .build();

        for (Map.Entry<DataType, Object> entry : childCollectionSamples.entrySet()) {
            DataType elementType = entry.getKey();
            Object elementSample = entry.getValue();

            tables.add(new TestTable(DataType.list(elementType), Lists.newArrayList(elementSample, elementSample), "2.1.3"));
            tables.add(new TestTable(DataType.set(elementType), Sets.newHashSet(elementSample), "2.1.3"));

            for (Map.Entry<DataType, Object> valueEntry : childCollectionSamples.entrySet()) {
                DataType valueType = valueEntry.getKey();
                Object valueSample = valueEntry.getValue();

                tables.add(new TestTable(DataType.map(elementType, valueType),
                    ImmutableMap.builder().put(elementSample, valueSample).build(), "2.1.3"));
            }
        }
        return tables;
    }

    private static Collection<? extends TestTable> tablesWithRandomlyGeneratedNestedCollections() {
        List<TestTable> tables = Lists.newArrayList();

        DataType nestedListType = buildNestedType(DataType.Name.LIST, 5);
        DataType nestedSetType = buildNestedType(DataType.Name.SET, 5);
        DataType nestedMapType = buildNestedType(DataType.Name.MAP, 5);

        tables.add(new TestTable(nestedListType, nestedObject(nestedListType), "2.1.3"));
        tables.add(new TestTable(nestedSetType, nestedObject(nestedSetType), "2.1.3"));
        tables.add(new TestTable(nestedMapType, nestedObject(nestedMapType), "2.1.3"));
        return tables;
    }

    /**
     * Populate a nested collection based on the given type and it's arguments.
     */
    public static Object nestedObject(DataType type) {

        int typeIdx = type.getTypeArguments().size() > 1 ? 1 : 0;
        DataType argument = type.getTypeArguments().get(typeIdx);
        boolean isAtBottom = !argument.isCollection();

        if(isAtBottom) {
            switch(type.getName()) {
                case LIST:
                    return Lists.newArrayList(1, 2, 3);
                case SET:
                    return Sets.newHashSet(1, 2, 3);
                case MAP:
                    Map<Integer, Integer> map = Maps.newHashMap();
                    map.put(1, 2);
                    map.put(3, 4);
                    map.put(5, 6);
                    return map;
            }
        }
        else {
            switch(type.getName()) {
                case LIST:
                    List<Object> l = Lists.newArrayListWithExpectedSize(2);
                    for(int i = 0; i < 5; i++) {
                        l.add(nestedObject(argument));
                    }
                    return l;
                case SET:
                    Set<Object> s = Sets.newHashSet();
                    for(int i = 0; i < 5; i++) {
                        s.add(nestedObject(argument));
                    }
                    return s;
                case MAP:
                    Map<Integer, Object> map = Maps.newHashMap();
                    for(int i = 0; i < 5; i++) {
                        map.put(i, nestedObject(argument));
                    }
                    return map;
            }
        }
        return null;
    }

    /**
     * @param baseType The base type to use, one of SET, MAP, LIST.
     * @param depth How many subcollections to generate.
     * @return a DataType that is a nested collection with the given baseType with the
     * given depth.
     */
    public static DataType buildNestedType(DataType.Name baseType, int depth) {
        Random r = new Random();
        DataType t = null;

        for (int i = 1; i <= depth; i++) {
            int chooser = r.nextInt(3);
            if(t == null) {
                if(chooser == 0) {
                    t = DataType.frozenList(DataType.cint());
                } else if(chooser == 1) {
                    t = DataType.frozenSet(DataType.cint());
                } else {
                    t = DataType.frozenMap(DataType.cint(), DataType.cint());
                }
            } else if(i == depth) {
                switch(baseType) {
                    case LIST:
                        return DataType.list(t);
                    case SET:
                        return DataType.set(t);
                    case MAP:
                        return DataType.map(DataType.cint(), t);
                }
            } else {
                if(chooser == 0) {
                    t = DataType.frozenList(t);
                } else if(chooser == 1) {
                    t = DataType.frozenSet(t);
                } else {
                    t = DataType.frozenMap(DataType.cint(), t);
                }
            }
        }
        return null;
    }
}
