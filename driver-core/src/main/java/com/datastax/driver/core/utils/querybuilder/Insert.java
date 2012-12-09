package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.TableMetadata;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * A built INSERT statement.
 */
public class Insert extends BuiltStatement {

    private boolean usingsProvided;

    Insert(String keyspace, String table, String[] columnNames, Object[] values) {
        super();
        init(keyspace, table, columnNames, values);
    }

    Insert(TableMetadata table, String[] columnNames, Object[] values) {
        super(table);
        init(table.getKeyspace().getName(), table.getName(), columnNames, values);
    }

    private void init(String keyspaceName, String tableName, String[] columnNames, Object[] values) {
        builder.append("INSERT INTO ");
        if (keyspaceName != null)
            appendName(keyspaceName).append(".");
        appendName(tableName);
        builder.append("(");
        Utils.joinAndAppendNames(builder, ",", columnNames);
        builder.append(") VALUES (");
        Utils.joinAndAppendValues(builder, ",", values);
        builder.append(")");

        for (int i = 0; i < columnNames.length; i++)
            maybeAddRoutingKey(columnNames[i], values[i]);
    }

    /**
     * Adds a USING clause to this statement.
     *
     * @param usings the options to use.
     * @return this statement.
     *
     * @throws IllegalStateException if a USING clause has already been
     * provided.
     */
    public Insert using(Using... usings) {
        if (usingsProvided)
            throw new IllegalStateException("A USING clause has already been provided");

        usingsProvided = true;

        if (usings.length == 0)
            return this;

        builder.append(" USING ");
        Utils.joinAndAppend(null, builder, " AND ", usings);
        return this;
    }

    public static class Builder {

        private final String[] columnNames;

        private TableMetadata tableMetadata;

        private String keyspace;
        private String table;

        Builder(String[] columnNames) {
            this.columnNames = columnNames;
        }

        /**
         * Sets the table to insert into.
         *
         * @param table the name of the table to insert into.
         * @return a new in-construction INSERT statement that inserts into {@code table}.
         */
        public Builder into(String table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("An INTO clause has already been provided");

            return into(null, table);
        }

        /**
         * Sets the table to insert into.
         *
         * @param keyspace the name of the keyspace to insert into.
         * @param table the name of the table to insert into.
         * @return a new in-construction INSERT statement that inserts into {@code keyspace.table}.
         */
        public Builder into(String keyspace, String table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("An INTO clause has already been provided");

            this.keyspace = keyspace;
            this.table = table;
            return this;
        }

        /**
         * Sets the table to insert into.
         *
         * @param table the name of the table to insert into.
         * @return a new in-construction INSERT statement that inserts into {@code table}.
         */
        public Builder into(TableMetadata table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("An INTO clause has already been provided");

            this.tableMetadata = table;
            return this;
        }

        /**
         * Specify the values to insert for the insert columns.
         *
         * @param values the values to insert. The {@code i}th value
         * corresponds to the {@code i}th column used when constructing this
         * {@code Insert.Builder object}.
         * @return the newly built INSERT statement.
         *
         * @throws IllegalArgumentException if the number of provided values
         * doesn't correspond to the number of columns used when constructing
         * this {@code Insert.Builder object}.
         * @throws IllegalStateException if no INTO clause have been defined.
         */
        public Insert values(Object... values) {
            if (values.length != columnNames.length)
                throw new IllegalArgumentException(String.format("Number of provided values (%d) doesn't match the number of inserted columns (%d)", values.length, columnNames.length));
            return build(values);
        }

        private Insert build(Object... values){
            return build(keyspace, table, tableMetadata, columnNames, values);
        }

        private static Insert build(String keyspace, String table, TableMetadata tableMetadata, String[] columnNames, Object[] values) {
            if (columnNames.length == 0)
                throw new IllegalStateException("Invalid empty columns list");
            if (tableMetadata != null)
                return new Insert(tableMetadata, columnNames, values);
            else if (table != null)
                return new Insert(keyspace, table, columnNames, values);
            else
                throw new IllegalStateException("Missing INTO clause");
        }

        public <T> ColumnsSpecification column(String columnName, T value) {
           return new ColumnsSpecification(this).add(columnName, value);
        }

        public static class ColumnsSpecification {
            private List<String> columnNames = new ArrayList<String>();
            private List<Object> values = new ArrayList<Object>();
            private final Builder builder ;

            ColumnsSpecification(Builder builder) {
               this.builder = builder;
            }

            <T> ColumnsSpecification add(String columnName, T value) {
                columnNames.add(columnName);
                values.add(value);
                return this;
            }

            public <T> ColumnsSpecification column(String columnName, T value) {
                return add(columnName, value);
            }

            /**
             * Syntaxic sugar
             * @return the unchange INSERT statement
             */
            public ColumnsSpecification and() {
                return this;
            }

            /**
             * @see Insert#using(Using...)
             */
            public Insert using(Using... usings) {
                return builder.build(
                       builder.keyspace,
                       builder.table,
                       builder.tableMetadata,
                       this.columnNames.toArray(new String[this.columnNames.size()]),
                       this.values.toArray(new Object[this.values.size()])
               ).using(usings);
            }
        }
    }
}
