package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.TableMetadata;

public class Delete extends BuiltStatement {

    Delete(String keyspace, String table, String[] columnNames, Clause[] clauses, Using[] usings) {
        super();
        init(keyspace, table, columnNames, clauses, usings);
    }

    Delete(TableMetadata table, String[] columnNames, Clause[] clauses, Using[] usings) {
        super(table);
        init(table.getKeyspace().getName(), table.getName(), columnNames, clauses, usings);
    }

    private void init(String keyspaceName, String tableName, String[] columnNames, Clause[] clauses, Using[] usings) {
        builder.append("DELETE ");
        Utils.joinAndAppendNames(builder, ",", columnNames);

        builder.append(" FROM ");
        if (keyspaceName != null)
            appendName(keyspaceName).append(".");
        appendName(tableName);

        if (usings != null && usings.length > 0) {
            builder.append(" USING ");
            Utils.joinAndAppend(null, builder, " AND ", usings);
        }

        builder.append(" WHERE ");
        Utils.joinAndAppend(this, builder, ",", clauses);
    }

    public static class Builder {

        private final String[] columnNames;

        private TableMetadata tableMetadata;

        private String keyspace;
        private String table;

        private Using[] usings;

        Builder(String[] columnNames) {
            this.columnNames = columnNames;
        }

        public Builder from(String table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("A FROM clause has already been provided");

            return from(null, table);
        }

        public Builder from(String keyspace, String table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("A FROM clause has already been provided");

            this.keyspace = keyspace;
            this.table = table;
            return this;
        }

        public Builder from(TableMetadata table) {
            if (table != null && tableMetadata != null)
                throw new IllegalStateException("A FROM clause has already been provided");

            this.tableMetadata = table;
            return this;
        }

        public Builder using(Using... usings) {
            if (this.usings != null)
                throw new IllegalStateException("A USING clause has already been provided");

            this.usings = usings;
            return this;
        }

        public Delete where(Clause... clauses) {
            return tableMetadata == null
                 ? new Delete(keyspace, table, columnNames, clauses, usings)
                 : new Delete(tableMetadata, columnNames, clauses, usings);
        }
    }
}
