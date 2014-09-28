package com.datastax.driver.core.schemabuilder;

import static java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.datastax.driver.core.DataType;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;

/**
 * A built CREATE TABLE statement
 */
public class Create extends SchemaStatement {

    private Optional<String> keyspaceName = Optional.absent();
    private String tableName;
    private Optional<Boolean> ifNotExists = Optional.absent();
    private Map<String, DataType> partitionColumnsMap = new LinkedHashMap<String, DataType>();
    private Map<String, DataType> clusteringColumnsMap = new LinkedHashMap<String, DataType>();
    private Map<String, DataType> simpleColumnsMap = new LinkedHashMap<String, DataType>();
    private Map<String, DataType> staticColumnsMap = new LinkedHashMap<String, DataType>();

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
     * Use 'IF NOT EXISTS' CAS condition for the table creation.
     *
     * @param ifNotExists whether to use the CAS condition.
     * @return this CREATE TABLE statement.
     */
    public Create ifNotExists(Boolean ifNotExists) {
        this.ifNotExists = Optional.fromNullable(ifNotExists);
        return this;
    }

    /**
     * Adds a columnName and dataType for a partition key.
     * <p>
     * Please note that partition keys are added in the order of their declaration;
     *
     * @param columnName the name of the partition key to be added;
     * @param dataType the data type of the partition key to be added.
     * @return this INSERT statement.
     */
    public Create addPartitionKey(String columnName, DataType dataType) {
        validateNotEmpty(tableName, "Partition key name");
        validateNotNull(dataType, "Partition key type");
        validateNotKeyWord(columnName, String.format("The partition key name '%s' is not allowed because it is a reserved keyword", columnName));
        partitionColumnsMap.put(columnName, dataType);
        return this;
    }

    /**
     * Adds a columnName and dataType for a clustering key.
     * <p>
     * Please note that clustering keys are added in the order of their declaration;
     *
     * @param columnName the name of the clustering key to be added;
     * @param dataType the data type of the clustering key to be added.
     * @return this INSERT statement.
     */
    public Create addClusteringKey(String columnName, DataType dataType) {
        validateNotEmpty(tableName, "Clustering key name");
        validateNotNull(dataType, "Clustering key type");
        validateNotKeyWord(columnName, String.format("The clustering key name '%s' is not allowed because it is a reserved keyword", columnName));
        clusteringColumnsMap.put(columnName, dataType);
        return this;
    }

    /**
     * Adds a columnName and dataType for a simple column.
     *
     * <p>
     *     To add a list column:
     *     <pre class="code"><code class="java">
     *         addColumn("myList",DataType.list(DataType.text()))
     *     </code></pre>
     *
     *     To add a set column:
     *     <pre class="code"><code class="java">
     *         addColumn("myList",DataType.set(DataType.text()))
     *     </code></pre>
     *
     *     To add a map column:
     *     <pre class="code"><code class="java">
     *         addColumn("myList",DataType.map(DataType.cint(),DataType.text()))
     *     </code></pre>
     * </p>
     * @param columnName the name of the column to be added;
     * @param dataType the data type of the column to be added.
     * @return this INSERT statement.
     *
     */
    public Create addColumn(String columnName, DataType dataType) {
        validateNotEmpty(tableName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumnsMap.put(columnName, dataType);
        return this;
    }

    /**
     * Shorthand method which takes a boolean and calls either {@code info.archinnov.achilles.schemabuilder.Create.addStaticColumn}
     * or {@code info.archinnov.achilles.schemabuilder.Create.addColumn}
     *
     * @param columnName  the name of the column to be added;
     * @param dataType the data type of the column to be added.
     * @param isStatic whether the column is static or not
     * @return this INSERT statement.
     */
    public Create addColumn(String columnName, DataType dataType, boolean isStatic) {
        if (isStatic) {
            addStaticColumn(columnName, dataType);
        } else {
            addColumn(columnName, dataType);
        }
        return this;
    }

    /**
     * Adds a <strong>static</strong> columnName and dataType for a simple column.
     *
     * @param columnName the name of the <strong>static</strong> column to be added;
     * @param dataType the data type of the column to be added.
     * @return this INSERT statement.
     */
    public Create addStaticColumn(String columnName, DataType dataType) {
        validateNotEmpty(tableName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The static column name '%s' is not allowed because it is a reserved keyword", columnName));
        staticColumnsMap.put(columnName, dataType);
        return this;
    }

    /**
     * Adds options for this CREATE TABLE statement.
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
            super(create);
            this.create = create;
        }

        private List<ClusteringOrder> clusteringOrderKeys = Collections.emptyList();

        private Optional<Boolean> compactStorage = Optional.absent();

        /**
         * Adds a {@link Create.Options.ClusteringOrder} for this table.
         * <p>
         * Please note that the {@link Create.Options.ClusteringOrder} are added in their declaration order
         *
         * @param clusteringOrders the list of {@link Create.Options.ClusteringOrder}.
         * @return this {@code Options} object
         */
        public Options clusteringOrder(ClusteringOrder... clusteringOrders) {
            if (clusteringOrders == null || clusteringOrders.length == 0) {
                throw new IllegalArgumentException(String.format("Cannot create table '%s' with null or empty clustering order keys", create.tableName));
            }
            for (ClusteringOrder clusteringOrder : clusteringOrders) {
                final String clusteringColumnName = clusteringOrder.getClusteringColumnName();
                if (!create.clusteringColumnsMap.containsKey(clusteringColumnName)) {
                    throw new IllegalArgumentException(String.format("Clustering key '%s' is unknown. Did you forget to declare it first ?", clusteringColumnName));
                }
            }
            clusteringOrderKeys = Arrays.asList(clusteringOrders);
            return this;
        }

        /**
         * Whether to use compact storage option for the table
         *
         * @param compactStorage whether to use compact storage.
         * @return this {@code Options} object
         */
        public Options compactStorage(Boolean compactStorage) {
            this.compactStorage = Optional.fromNullable(compactStorage);
            return this;
        }


        /**
         * Defines a clustering order
         */
        public static class ClusteringOrder {
            private String clusteringColumnName;
            private Sorting sorting = Sorting.ASC;


            /**
             * Create a new clustering ordering with colummName/order
             *
             * @param clusteringColumnName the clustering column.
             * @param sorting the {@link Create.Options.ClusteringOrder.Sorting}. Possible values are: ASC, DESC.
             */
            public ClusteringOrder(String clusteringColumnName, Sorting sorting) {
                validateNotEmpty(clusteringColumnName, "Column name for clustering order");
                this.clusteringColumnName = clusteringColumnName;
                this.sorting = sorting;
            }

            /**
             * Create a new clustering ordering with colummName and default ASC sorting order
             *
             * @param clusteringColumnName the clustering column.
             */
            public ClusteringOrder(String clusteringColumnName) {
                validateNotEmpty(clusteringColumnName, "Column name for clustering order");
                this.clusteringColumnName = clusteringColumnName;
            }

            public String getClusteringColumnName() {
                return clusteringColumnName;
            }

            public Sorting getSorting() {
                return sorting;
            }

            public String toStatement() {
                return clusteringColumnName + " " + sorting.name();
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
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                ClusteringOrder that = (ClusteringOrder) o;

                if (clusteringColumnName != null ? !clusteringColumnName.equals(that.clusteringColumnName) : that.clusteringColumnName != null) {
                    return false;
                }
                if (sorting != that.sorting) {
                    return false;
                }

                return true;
            }

            @Override
            public int hashCode() {
                int result = clusteringColumnName != null ? clusteringColumnName.hashCode() : 0;
                result = 31 * result + (sorting != null ? sorting.hashCode() : 0);
                return result;
            }

            /**
             * The clustering sorting order
             */
            public static enum Sorting {
                ASC, DESC
            }
        }


        @Override
        String buildOptions() {
            final List<String> commonOptions = super.buildCommonOptions();
            List<String> options = new ArrayList<String>(commonOptions);

            if (!clusteringOrderKeys.isEmpty()) {
                options.add(new StringBuilder(CLUSTERING_ORDER_BY).append(OPEN_PAREN).append(Joiner.on(SUB_OPTION_SEPARATOR).join(clusteringOrderKeys)).append(CLOSE_PAREN).toString());
            }

            if (compactStorage.isPresent() && compactStorage.get()) {
                if (!create.staticColumnsMap.isEmpty()) {
                    throw new IllegalStateException(String.format("Cannot create table '%s' with compact storage and static columns '%s'", create.tableName, create.staticColumnsMap.keySet()));
                }
                options.add(COMPACT_STORAGE);
            }

            return new StringBuilder(NEW_LINE).append(TAB).append(WITH).append(SPACE).append(Joiner.on(OPTION_SEPARATOR).join(options)).toString();
        }

        /**
         * Generate the final CREATE TABLE statement <strong>with</strong> table options
         *
         * @return the final CREATE TABLE statement <strong>with</strong> table options
         */
        @Override
        public String build() {
            StringBuilder statement = new StringBuilder(super.build());
            statement.append(this.buildOptions());
            return statement.toString();
        }
    }

    @Override
    String buildInternal() {
        if (partitionColumnsMap.size() < 1) {
            throw new IllegalStateException(String.format("There should be at least one partition key defined for the table '%s'", tableName));
        }

        validateColumnsDeclaration();

        StringBuilder createStatement = new StringBuilder(NEW_LINE).append(TAB).append(CREATE_TABLE);
        if (ifNotExists.isPresent() && ifNotExists.get()) {
            createStatement.append(SPACE).append(IF_NOT_EXISTS);
        }
        createStatement.append(SPACE);
        if (keyspaceName.isPresent()) {
            createStatement.append(keyspaceName.get()).append(DOT);
        }
        createStatement.append(tableName);

        List<String> allColumns = new ArrayList<String>();
        List<String> partitionKeyColumns = new ArrayList<String>();
        List<String> clusteringKeyColumns = new ArrayList<String>();


        for (Entry<String, DataType> entry : partitionColumnsMap.entrySet()) {
            allColumns.add(new StringBuilder(entry.getKey()).append(SPACE).append(entry.getValue().getName().toString()).toString());
            partitionKeyColumns.add(entry.getKey());
        }

        for (Entry<String, DataType> entry : clusteringColumnsMap.entrySet()) {
            allColumns.add(new StringBuilder(entry.getKey()).append(SPACE).append(entry.getValue().getName().toString()).toString());
            clusteringKeyColumns.add(entry.getKey());
        }

        for (Entry<String, DataType> entry : staticColumnsMap.entrySet()) {
            final StringBuilder column = new StringBuilder();
            buildColumnType(entry, column);
            column.append(SPACE).append(STATIC);
            allColumns.add(column.toString());
        }

        for (Entry<String, DataType> entry : simpleColumnsMap.entrySet()) {
            final StringBuilder column = new StringBuilder();
            buildColumnType(entry, column);
            allColumns.add(column.toString());
        }

        String partitionKeyPart = partitionKeyColumns.size() == 1 ?
                partitionKeyColumns.get(0)
                : new StringBuilder(OPEN_PAREN).append(Joiner.on(SEPARATOR).join(partitionKeyColumns)).append(CLOSE_PAREN).toString();

        String primaryKeyPart = clusteringKeyColumns.size() == 0 ?
                partitionKeyPart
                : new StringBuilder(partitionKeyPart).append(SEPARATOR).append(Joiner.on(SEPARATOR).join(clusteringKeyColumns)).toString();

        createStatement.append(OPEN_PAREN).append(NEW_LINE).append(TAB).append(TAB);
        createStatement.append(Joiner.on(COLUMN_FORMATTING).join(allColumns));
        createStatement.append(COLUMN_FORMATTING).append(PRIMARY_KEY);
        createStatement.append(OPEN_PAREN).append(primaryKeyPart).append(CLOSE_PAREN);
        createStatement.append(CLOSE_PAREN);

        return createStatement.toString();
    }

    private void buildColumnType(Entry<String, DataType> entry, StringBuilder column) {
        final DataType dataType = entry.getValue();
        final String type = dataType.getName().toString();
        column.append(entry.getKey()).append(SPACE).append(type);

        if (dataType.isCollection()) {
            final List<DataType> typeArguments = dataType.getTypeArguments();
            if (typeArguments.size() == 1) {
                column.append(OPEN_TYPE).append(typeArguments.get(0)).append(CLOSE_TYPE);
            } else if (typeArguments.size() == 2) {
                column.append(OPEN_TYPE).append(typeArguments.get(0)).append(COMMA).append(typeArguments.get(1)).append(CLOSE_TYPE);
            }
        }
    }

    /**
     * Generate a CREATE TABLE statement <strong>without</strong> table options
     * @return the final CREATE TABLE statement
     */
    public String build() {
        return buildInternal();
    }

    private void validateColumnsDeclaration() {

        final Collection partitionAndClusteringColumns = this.intersection(partitionColumnsMap.keySet(), clusteringColumnsMap.keySet());
        final Collection partitionAndSimpleColumns = this.intersection(partitionColumnsMap.keySet(), simpleColumnsMap.keySet());
        final Collection clusteringAndSimpleColumns = this.intersection(clusteringColumnsMap.keySet(), simpleColumnsMap.keySet());
        final Collection partitionAndStaticColumns = this.intersection(partitionColumnsMap.keySet(), staticColumnsMap.keySet());
        final Collection clusteringAndStaticColumns = this.intersection(clusteringColumnsMap.keySet(), staticColumnsMap.keySet());
        final Collection simpleAndStaticColumns = this.intersection(simpleColumnsMap.keySet(), staticColumnsMap.keySet());

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

        if (!staticColumnsMap.isEmpty() && clusteringColumnsMap.isEmpty()) {
            throw new IllegalStateException(String.format("The table '%s' cannot declare static columns '%s' without clustering columns", tableName, staticColumnsMap.keySet()));
        }
    }

    private <T> Collection<T> intersection(Collection<T> col1, Collection<T> col2) {
        Set<T> set = new HashSet<T>();

        for (T t : col1) {
            if(col2.contains(t)) {
                set.add(t);
            }
        }
        return set;
    }


}
