package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.TableMetadata;

/**
 * A built UPDATE statement.
 */
public class Update extends BuiltStatement {

    Update(String keyspace, String table, Assignment[] assignments, Clause[] clauses, Using[] usings) {
        super();
        init(keyspace, table, assignments, clauses, usings);
    }

    Update(TableMetadata table, Assignment[] assignments, Clause[] clauses, Using[] usings) {
        super(table);
        init(table.getKeyspace().getName(), table.getName(), assignments, clauses, usings);
    }

    private void init(String keyspaceName, String tableName, Assignment[] assignments, Clause[] clauses, Using[] usings) {
        builder.append("UPDATE ");
        if (keyspaceName != null)
            appendName(keyspaceName).append(".");
        appendName(tableName);

        if (usings != null && usings.length > 0) {
            builder.append(" USING ");
            Utils.joinAndAppend(null, builder, " AND ", usings);
        }

        builder.append(" SET ");
        Utils.joinAndAppend(null, builder, ",", assignments);

        builder.append(" WHERE ");
        Utils.joinAndAppend(this, builder, ",", clauses);
    }

    public static class Builder {

        private final TableMetadata tableMetadata;

        private final String keyspace;
        private final String table;

        private Assignment[] assignments;
        private Using[] usings;

        Builder(String keyspace, String table) {
            this.keyspace = keyspace;
            this.table = table;
            this.tableMetadata = null;
        }

        Builder(TableMetadata tableMetadata) {
            this.tableMetadata = tableMetadata;
            this.keyspace = null;
            this.table = null;
        }

        /**
         * Adds a USING clause to this statement.
         *
         * @param usings the options to use.
         * @return this builderj.
         *
         * @throws IllegalStateException if a USING clause has already been
         * provided.
         */
        public Builder using(Using... usings) {
            if (this.usings != null)
                throw new IllegalStateException("A USING clause has already been provided");

            this.usings = usings;
            return this;
        }

        /**
         * Adds the columns modification/assignement to set with this UPDATE
         * statement.
         *
         * @param assignments the assigments to set for this statement.
         * @return this builder.
         *
         * @throws IllegalStateException if a SET clause has aready been provided.
         */
        public Builder set(Assignment... assignments) {
            if (this.assignments != null)
                throw new IllegalStateException("A SET clause has already been provided");

            this.assignments = assignments;
            return this;
        }

        /**
         * Adds a WHERE clause to this statement.
         *
         * @param clauses the clause to add.
         * @return the newly built UPDATE statement.
         *
         * @throws IllegalStateException if WHERE clauses have already been
         * provided.
         */
        public Update where(Clause... clauses) {

            if (tableMetadata != null)
                return new Update(tableMetadata, assignments, clauses, usings);
            else if (table != null)
                return new Update(keyspace, table, assignments, clauses, usings);
            else
                throw new IllegalStateException("Missing SET clause");
        }
    }
}
