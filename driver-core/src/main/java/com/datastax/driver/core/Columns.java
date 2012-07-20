package com.datastax.driver.core;

/**
 * Metadata describing the columns returned in a {@link ResultSet} or a
 * {@link PreparedStatement}.
 */
public class Columns {

    /**
     * Returns the number of columns described by this {@code Columns}
     * instance.
     *
     * @return the number of columns described by this metadata.
     */
    public int count() {
        return 0;
    }

    /**
     * Returns the name of the {@code i}th column in this metadata.
     *
     * @return the name of the {@code i}th column in this metadata.
     */
    public String name(int i) {
        return null;
    }

    /**
     * Returns the type of the {@code i}th column in this metadata.
     *
     * @return the type of the {@code i}th column in this metadata.
     */
    public DataType type(int i) {
        return null;
    }

    /**
     * Returns the type of column {@code name} in this metadata.
     *
     * @return the type of column {@code name} in this metadata.
     */
    public DataType type(String name) {
        return null;
    }

    /**
     * Returns the keyspace of the {@code i}th column in this metadata.
     *
     * @return the keyspace of the {@code i}th column in this metadata.
     */
    public String keyspace(int i) {
        return null;
    }

    /**
     * Returns the keyspace of column {@code name} in this metadata.
     *
     * @return the keyspace of column {@code name} in this metadata.
     */
    public String keyspace(String name) {
        return null;
    }

    /**
     * Returns the table of the {@code i}th column in this metadata.
     *
     * @return the table of the {@code i}th column in this metadata.
     */
    public String table(int i) {
        return null;
    }

    /**
     * Returns the table of column {@code name} in this metadata.
     *
     * @return the table of column {@code name} in this metadata.
     */
    public String table(String name) {
        return null;
    }

}
