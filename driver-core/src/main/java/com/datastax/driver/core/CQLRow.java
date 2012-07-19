package com.datastax.driver.core;

import java.util.Date;

/**
 * A CQL Row returned in a {@link ResultSet}.
 */
public class CQLRow {

    /**
     * The columns contains in this CQLRow.
     *
     * @return the columns contained in this CQLRow.
     */
    public Columns columns() {
        return null;
    }

    public boolean getBool(int i) {
        return false;
    }

    public boolean getBool(String name) {
        return false;
    }

    public int getInt(int i) {
        return 0;
    }

    public int getInt(String name) {
        return 0;
    }

    public long getLong(int i) {
        return 0;
    }

    public long getLong(String name) {
        return 0;
    }

    public Date getDate(int i) {
        return null;
    }

    public Date getDate(String name) {
        return null;
    }

    // ...
}
