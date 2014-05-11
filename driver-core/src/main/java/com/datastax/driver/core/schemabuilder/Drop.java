package com.datastax.driver.core.schemabuilder;

import com.google.common.base.Optional;

/**
 * A built DROP TABLE statement
 */
public class Drop extends SchemaStatement {

    private Optional<String> keyspaceName = Optional.absent();
    private String tableName;
    private Optional<Boolean> ifExists = Optional.absent();

    Drop(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        this.tableName = tableName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    Drop(String tableName) {
        validateNotEmpty(tableName, "Table name");
        this.tableName = tableName;
    }

    /**
     * Use 'IF EXISTS' CAS condition for the table drop.
     *
     * @param ifExists whether to use the CAS condition.
     * @return a new {@link com.datastax.driver.core.schemabuilder.Drop} instance.
     */
    public Drop ifExists(Boolean ifExists) {
        this.ifExists = Optional.fromNullable(ifExists);
        return this;
    }

    @Override
    String buildInternal() {
        StringBuilder dropStatement = new StringBuilder("DROP TABLE");
        if (ifExists.isPresent() && ifExists.get()) {
            dropStatement.append(SPACE).append("IF EXISTS");
        }
        dropStatement.append(SPACE);
        if (keyspaceName.isPresent()) {
            dropStatement.append(keyspaceName.get()).append(DOT);
        }

        dropStatement.append(tableName);
        return dropStatement.toString();
    }

    /**
     * Generate a DROP TABLE statement
     * @return the final DROP TABLE statement
     */
    public String build() {
        return this.buildInternal();
    }
}
