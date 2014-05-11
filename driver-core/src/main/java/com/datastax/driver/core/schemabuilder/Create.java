package com.datastax.driver.core.schemabuilder;

import static java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    Create(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        this.tableName = tableName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    Create(String tableName) {
        validateNotEmpty(tableName, "Table name");
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
        clusteringColumnsMap.put(columnName, dataType);
        return this;
    }

    /**
     * Adds a columnName and dataType for a simple column.
     *
     * @param columnName the name of the column to be added;
     * @param dataType the data type of the column to be added.
     * @return this INSERT statement.
     */
    public Create addColumn(String columnName, DataType dataType) {
        validateNotEmpty(tableName, "Column name");
        validateNotNull(dataType, "Column type");
        simpleColumnsMap.put(columnName, dataType);
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
         * Adds a {@link com.datastax.driver.core.schemabuilder.Create.Options.ClusteringOrder} for this table.
         * <p>
         * Please note that the {@link com.datastax.driver.core.schemabuilder.Create.Options.ClusteringOrder} are added in their declaration order
         *
         * @param clusteringOrders the list of {@link com.datastax.driver.core.schemabuilder.Create.Options.ClusteringOrder}.
         * @return this {@code Options} object
         */
        public Options clusteringOrder(ClusteringOrder... clusteringOrders) {
            if (clusteringOrders == null || clusteringOrders.length == 0) {
                throw new IllegalArgumentException(String.format("Cannot create table '%s' with null or empty clustering order keys", create.tableName));
            }
            for (ClusteringOrder clusteringOrder : clusteringOrders) {
                final String clusteringColumnName = clusteringOrder.getClusteringColumnName();
                if (!create.clusteringColumnsMap.containsKey(clusteringColumnName)) {
                    throw new IllegalArgumentException(String.format("Clustering order key '%s' is unknown. Did you forget to declare it first ?", clusteringColumnName));
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
             * @param sorting the {@link com.datastax.driver.core.schemabuilder.Create.Options.ClusteringOrder.Sorting}. Possible values are: ASC, DESC.
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

            private String getClusteringColumnName() {
                return clusteringColumnName;
            }

            public String toStatement() {
                return clusteringColumnName + " " + sorting.name();
            }

            @Override
            public String toString() {
                return toStatement();
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
                options.add(new StringBuilder("CLUSTERING ORDER BY").append(OPEN_PAREN).append(Joiner.on(SUB_OPTION_SEPARATOR).join(clusteringOrderKeys)).append(CLOSE_PAREN).toString());
            }

            if (compactStorage.isPresent() && compactStorage.get()) {
                options.add("COMPACT STORAGE");
            }
            return new StringBuilder(" WITH ").append(Joiner.on(OPTION_SEPARATOR).join(options)).toString();
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

        StringBuilder createStatement = new StringBuilder("CREATE TABLE");
        if (ifNotExists.isPresent() && ifNotExists.get()) {
            createStatement.append(SPACE).append("IF NOT EXISTS");
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

        for (Entry<String, DataType> entry : simpleColumnsMap.entrySet()) {
            allColumns.add(new StringBuilder(entry.getKey()).append(SPACE).append(entry.getValue().getName().toString()).toString());
        }

        String partitionKeyPart = partitionKeyColumns.size() == 1 ?
                partitionKeyColumns.get(0)
                : new StringBuilder(OPEN_PAREN).append(Joiner.on(SEPARATOR).join(partitionKeyColumns)).append(CLOSE_PAREN).toString();

        String primaryKeyPart = clusteringKeyColumns.size() == 0 ?
                partitionKeyPart
                : new StringBuilder(partitionKeyPart).append(SEPARATOR).append(Joiner.on(SEPARATOR).join(clusteringKeyColumns)).toString();

        createStatement.append(OPEN_PAREN);
        createStatement.append(Joiner.on(SEPARATOR).join(allColumns));
        createStatement.append(SEPARATOR).append("PRIMARY KEY");
        createStatement.append(OPEN_PAREN).append(primaryKeyPart).append(CLOSE_PAREN);
        createStatement.append(CLOSE_PAREN);

        return createStatement.toString();
    }

    /**
     * Generate a CREATE TABLE statement <strong>without</strong> table options
     * @return the final CREATE TABLE statement
     */
    public String build() {
        return buildInternal();
    }
}
