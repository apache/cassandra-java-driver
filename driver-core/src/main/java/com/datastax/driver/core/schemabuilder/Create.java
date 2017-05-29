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
package com.datastax.driver.core.schemabuilder;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.utils.MoreObjects;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import java.util.*;

import static java.util.Map.Entry;

/**
 * A built CREATE TABLE statement.
 */
public class Create extends AbstractCreateStatement<Create> {

    private String tableName;
    private Map<String, ColumnType> partitionColumns = new LinkedHashMap<String, ColumnType>();
    private Map<String, ColumnType> clusteringColumns = new LinkedHashMap<String, ColumnType>();
    private Map<String, ColumnType> staticColumns = new LinkedHashMap<String, ColumnType>();

    Create(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    Create(String tableName) {
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
    }

    /**
     * Add a partition key column definition to this CREATE TABLE statement.
     * <p/>
     * This includes the column declaration (you don't need an additional {@code addColumn} call).
     * <p/>
     * Partition key columns are added in the order of their declaration.
     *
     * @param columnName the name of the partition key column to be added.
     * @param dataType   the data type of the partition key column to be added.
     * @return this CREATE statement.
     */
    public Create addPartitionKey(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Partition key name");
        validateNotNull(dataType, "Partition key type");
        validateNotKeyWord(columnName, String.format("The partition key name '%s' is not allowed because it is a reserved keyword", columnName));
        partitionColumns.put(columnName, new NativeColumnType(dataType));
        return this;
    }

    /**
     * Add a partition key column definition to this CREATE TABLE statement, when its type contains a UDT.
     * <p/>
     * This includes the column declaration (you don't need an additional {@code addColumn} call).
     * <p/>
     * Partition key columns are added in the order of their declaration.
     *
     * @param columnName the name of the partition key column to be added.
     * @param udtType    the UDT type of the partition key column to be added. Use {@link SchemaBuilder#frozen(String)} or {@link SchemaBuilder#udtLiteral(String)}.
     * @return this CREATE statement.
     */
    public Create addUDTPartitionKey(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Clustering key name");
        validateNotNull(udtType, "UDT partition key type");
        validateNotKeyWord(columnName, String.format("The partition key name '%s' is not allowed because it is a reserved keyword", columnName));
        partitionColumns.put(columnName, udtType);
        return this;
    }

    /**
     * Add a clustering column definition to this CREATE TABLE statement.
     * <p/>
     * This includes the column declaration (you don't need an additional {@code addColumn} call).
     * <p/>
     * Clustering columns are added in the order of their declaration.
     *
     * @param columnName the name of the clustering column to be added.
     * @param dataType   the data type of the clustering column to be added.
     * @return this CREATE statement.
     */
    public Create addClusteringColumn(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Clustering column name");
        validateNotNull(dataType, "Clustering column type");
        validateNotKeyWord(columnName, String.format("The clustering column name '%s' is not allowed because it is a reserved keyword", columnName));
        clusteringColumns.put(columnName, new NativeColumnType(dataType));
        return this;
    }

    /**
     * Add a clustering column definition to this CREATE TABLE statement, when its type contains a UDT.
     * <p/>
     * This includes the column declaration (you don't need an additional {@code addColumn} call).
     * <p/>
     * Clustering columns are added in the order of their declaration.
     *
     * @param columnName the name of the clustering column to be added.
     * @param udtType    the UDT type of the clustering column to be added. Use {@link SchemaBuilder#frozen(String)} or {@link SchemaBuilder#udtLiteral(String)}.
     * @return this CREATE statement.
     */
    public Create addUDTClusteringColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Clustering column name");
        validateNotNull(udtType, "UDT clustering column type");
        validateNotKeyWord(columnName, String.format("The clustering column name '%s' is not allowed because it is a reserved keyword", columnName));
        clusteringColumns.put(columnName, udtType);
        return this;
    }

    /**
     * Add a static column definition to this CREATE TABLE statement.
     *
     * @param columnName the name of the column to be added.
     * @param dataType   the data type of the column to be added.
     * @return this CREATE statement.
     */
    public Create addStaticColumn(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The static column name '%s' is not allowed because it is a reserved keyword", columnName));
        staticColumns.put(columnName, new NativeColumnType(dataType));
        return this;
    }

    /**
     * Add a static column definition to this CREATE TABLE statement, when its type contains a UDT.
     *
     * @param columnName the name of the column to be added.
     * @param udtType    the UDT type of the column to be added. Use {@link SchemaBuilder#frozen(String)} or {@link SchemaBuilder#udtLiteral(String)}.
     * @return this CREATE statement.
     */
    public Create addUDTStaticColumn(String columnName, UDTType udtType) {
        validateNotEmpty(tableName, "Column name");
        validateNotNull(udtType, "Column UDT type");
        validateNotKeyWord(columnName, String.format("The static column name '%s' is not allowed because it is a reserved keyword", columnName));
        staticColumns.put(columnName, udtType);
        return this;
    }

    /**
     * Add options for this CREATE TABLE statement.
     *
     * @return the options of this CREATE TABLE statement.
     */
    public Options withOptions() {
        return new Options(this);
    }

    /**
     * The table options of a CREATE TABLE statement.
     */
    public static class Options extends TableOptions<Options> {

        private final Create create;

        private Options(Create create) {
            super(create.asStatementStart());
            this.create = create;
        }

        private List<ClusteringOrder> clusteringOrderKeys = Lists.newArrayList();

        private boolean compactStorage;

        /**
         * Add a clustering order for this table.
         * <p/>
         * To define the order on multiple columns, call this method repeatedly. The columns will be declared in the call order.
         *
         * @param columnName the clustering column name.
         * @param direction  the clustering direction (DESC/ASC).
         * @return this {@code Options} object.
         */
        public Options clusteringOrder(String columnName, SchemaBuilder.Direction direction) {
            if (!create.clusteringColumns.containsKey(columnName)) {
                throw new IllegalArgumentException(String.format("Clustering key '%s' is unknown. Did you forget to declare it first?", columnName));
            }
            clusteringOrderKeys.add(new ClusteringOrder(columnName, direction));
            return this;
        }

        /**
         * Enable the compact storage option for the table.
         *
         * @return this {@code Options} object.
         */
        public Options compactStorage() {
            this.compactStorage = true;
            return this;
        }

        private static class ClusteringOrder {
            private final String clusteringColumnName;
            private final SchemaBuilder.Direction direction;

            ClusteringOrder(String clusteringColumnName, SchemaBuilder.Direction direction) {
                validateNotEmpty(clusteringColumnName, "Column name for clustering order");
                this.clusteringColumnName = clusteringColumnName;
                this.direction = direction;
            }

            public String getClusteringColumnName() {
                return clusteringColumnName;
            }

            public String toStatement() {
                return clusteringColumnName + " " + direction.name();
            }

            @Override
            public String toString() {
                return toStatement();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o instanceof ClusteringOrder) {
                    ClusteringOrder that = (ClusteringOrder) o;
                    return MoreObjects.equal(this.clusteringColumnName, that.clusteringColumnName) && MoreObjects.equal(this.direction, that.direction);
                }
                return false;
            }

            @Override
            public int hashCode() {
                return MoreObjects.hashCode(clusteringColumnName, direction);
            }
        }

        @Override
        protected void addSpecificOptions(List<String> options) {
            if (!clusteringOrderKeys.isEmpty()) {
                options.add("CLUSTERING ORDER BY(" + Joiner.on(", ").join(clusteringOrderKeys) + ")");
            }

            if (compactStorage) {
                if (!create.staticColumns.isEmpty()) {
                    throw new IllegalStateException(String.format("Cannot create table '%s' with compact storage and static columns '%s'", create.tableName, create.staticColumns.keySet()));
                }
                options.add("COMPACT STORAGE");
            }
        }
    }

    @Override
    public String buildInternal() {
        if (partitionColumns.size() < 1) {
            throw new IllegalStateException(String.format("There should be at least one partition key defined for the table '%s'", tableName));
        }

        validateColumnsDeclaration();

        StringBuilder createStatement = new StringBuilder(STATEMENT_START).append("CREATE TABLE");
        if (ifNotExists) {
            createStatement.append(" IF NOT EXISTS");
        }
        createStatement.append(" ");
        if (keyspaceName.isPresent()) {
            createStatement.append(keyspaceName.get()).append(".");
        }
        createStatement.append(tableName);

        List<String> allColumns = new ArrayList<String>();
        List<String> partitionKeyColumns = new ArrayList<String>();
        List<String> clusteringKeyColumns = new ArrayList<String>();

        for (Entry<String, ColumnType> entry : partitionColumns.entrySet()) {
            allColumns.add(entry.getKey() + " " + entry.getValue().asCQLString());
            partitionKeyColumns.add(entry.getKey());
        }

        for (Entry<String, ColumnType> entry : clusteringColumns.entrySet()) {
            allColumns.add(entry.getKey() + " " + entry.getValue().asCQLString());
            clusteringKeyColumns.add(entry.getKey());
        }

        for (Entry<String, ColumnType> entry : staticColumns.entrySet()) {
            allColumns.add(entry.getKey() + " " + entry.getValue().asCQLString() + " static");
        }

        for (Entry<String, ColumnType> entry : simpleColumns.entrySet()) {
            allColumns.add(buildColumnType(entry));
        }

        String partitionKeyPart = partitionKeyColumns.size() == 1 ?
                partitionKeyColumns.get(0)
                : "(" + Joiner.on(", ").join(partitionKeyColumns) + ")";

        String primaryKeyPart = clusteringKeyColumns.size() == 0 ?
                partitionKeyPart
                : partitionKeyPart + ", " + Joiner.on(", ").join(clusteringKeyColumns);

        createStatement.append("(").append(COLUMN_FORMATTING);
        createStatement.append(Joiner.on("," + COLUMN_FORMATTING).join(allColumns));
        createStatement.append("," + COLUMN_FORMATTING).append("PRIMARY KEY");
        createStatement.append("(").append(primaryKeyPart).append(")");
        createStatement.append(")");

        return createStatement.toString();
    }

    private void validateColumnsDeclaration() {

        final Collection partitionAndClusteringColumns = this.intersection(partitionColumns.keySet(), clusteringColumns.keySet());
        final Collection partitionAndSimpleColumns = this.intersection(partitionColumns.keySet(), simpleColumns.keySet());
        final Collection clusteringAndSimpleColumns = this.intersection(clusteringColumns.keySet(), simpleColumns.keySet());
        final Collection partitionAndStaticColumns = this.intersection(partitionColumns.keySet(), staticColumns.keySet());
        final Collection clusteringAndStaticColumns = this.intersection(clusteringColumns.keySet(), staticColumns.keySet());
        final Collection simpleAndStaticColumns = this.intersection(simpleColumns.keySet(), staticColumns.keySet());

        if (!partitionAndClusteringColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as partition keys and clustering keys at the same time", partitionAndClusteringColumns));
        }

        if (!partitionAndSimpleColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as partition keys and simple columns at the same time", partitionAndSimpleColumns));
        }

        if (!clusteringAndSimpleColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as clustering keys and simple columns at the same time", clusteringAndSimpleColumns));
        }

        if (!partitionAndStaticColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as partition keys and static columns at the same time", partitionAndStaticColumns));
        }

        if (!clusteringAndStaticColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as clustering keys and static columns at the same time", clusteringAndStaticColumns));
        }

        if (!simpleAndStaticColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as simple columns and static columns at the same time", simpleAndStaticColumns));
        }

        if (!staticColumns.isEmpty() && clusteringColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The table '%s' cannot declare static columns '%s' without clustering columns", tableName, staticColumns.keySet()));
        }
    }

    private <T> Collection<T> intersection(Collection<T> col1, Collection<T> col2) {
        Set<T> set = new HashSet<T>();

        for (T t : col1) {
            if (col2.contains(t)) {
                set.add(t);
            }
        }
        return set;
    }
}
