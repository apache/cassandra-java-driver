package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.TableMetadata;

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
            if (columnNames.length == 0)
                throw new IllegalArgumentException("Invalid empty column names");

            this.columnNames = columnNames;
        }

        public Builder into(String table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("An INTO clause has already been provided");

            return into(null, table);
        }

        public Builder into(String keyspace, String table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("An INTO clause has already been provided");

            this.keyspace = keyspace;
            this.table = table;
            return this;
        }

        public Builder into(TableMetadata table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("An INTO clause has already been provided");

            this.tableMetadata = table;
            return this;
        }

        public Insert values(Object... values) {

            if (values.length != columnNames.length)
                throw new IllegalArgumentException(String.format("Number of provided values (%d) doesn't match the number of inserted columns (%d)", values.length, columnNames.length));

            return tableMetadata == null
                 ? new Insert(keyspace, table, columnNames, values)
                 : new Insert(tableMetadata, columnNames, values);
        }
    }
}
