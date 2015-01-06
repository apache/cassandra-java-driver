package com.datastax.driver.mapping;


import com.datastax.driver.core.querybuilder.Assignment;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * UpdatePolicy determines how columns are updated in Cassandra
 * @see com.datastax.driver.mapping.annotations.Column
 */
public enum UpdatePolicy {

    /**
     * The default UpdatePolicy, replaces the existing value that's currently stored in Cassandra
     */
    OVERWRITE {
        @Override
        public Assignment preparedAssignment(ColumnMapper<?> cm) {
            return set(cm.getColumnName(), bindMarker());
        }
    },

    /**
     * For Collection types, append a value to what's currently stored in Cassandra
     */
    APPEND {
        @Override
        public Assignment preparedAssignment(ColumnMapper<?> cm) {
            return append(cm.getColumnName(), bindMarker());
        }
    },

    /**
     * For List types, adds a value to the head of the List with what's currently stored in Cassandra
     */
    PREPEND {
        @Override
        public Assignment preparedAssignment(ColumnMapper<?> cm) {
            return prepend(cm.getColumnName(), bindMarker());
        }
    };

    /**
     * Using the QueryBuilder, returns a preparedStatement Assignment for the given ColumnMapper
     * @param cm The ColumnMapper to apply the Assignment to
     * @return the Assignment
     */
    public abstract Assignment preparedAssignment(ColumnMapper<?> cm);
}
