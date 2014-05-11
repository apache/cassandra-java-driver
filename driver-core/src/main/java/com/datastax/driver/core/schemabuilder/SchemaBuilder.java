package com.datastax.driver.core.schemabuilder;

/**
 * Static methods to build a CQL3 DML statement.
 * <p>
 * Available DML statements: CREATE/ALTER/DROP
 * <p>
 * Note that it could be convenient to use an 'import static' to use the methods of this class.
 */
public final class SchemaBuilder {

    private SchemaBuilder() {
    }

    /**
     * Start building a new CREATE TABLE statement
     *
     * @param tableName the name of the table to create.
     * @return an in-construction CREATE statement.
     */
    public static Create createTable(String tableName) {
        return new Create(tableName);
    }

    /**
     * Start building a new CREATE TABLE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param tableName the name of the table to create.
     * @return an in-construction CREATE statement.
     */
    public static Create createTable(String keyspaceName, String tableName) {
        return new Create(keyspaceName, tableName);
    }

    /**
     * Start building a new ALTER TABLE statement
     *
     * @param tableName the name of the table to be altered.
     * @return an in-construction ALTER statement.
     */
    public static Alter alterTable(String tableName) {
        return new Alter(tableName);
    }

    /**
     * Start building a new ALTER TABLE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param tableName the name of the table to be altered.
     * @return an in-construction ALTER statement.
     */
    public static Alter alterTable(String keyspaceName, String tableName) {
        return new Alter(keyspaceName, tableName);
    }

    /**
     * Start building a new DROP TABLE statement
     *
     * @param tableName the name of the table to be dropped.
     * @return an in-construction DROP statement.
     */
    public static Drop dropTable(String tableName) {
        return new Drop(tableName);
    }

    /**
     * Start building a new DROP TABLE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param tableName the name of the table to be dropped.
     * @return an in-construction DROP statement.
     */
    public static Drop dropTable(String keyspaceName, String tableName) {
        return new Drop(keyspaceName, tableName);
    }


}
