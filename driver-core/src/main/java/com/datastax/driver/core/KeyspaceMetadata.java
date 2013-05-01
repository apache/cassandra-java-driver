/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.concurrent.ConcurrentHashMap;

import java.util.*;

/**
 * Describes a keyspace defined in this cluster.
 */
public class KeyspaceMetadata {

    public static final String KS_NAME           = "keyspace_name";
    private static final String DURABLE_WRITES   = "durable_writes";
    private static final String STRATEGY_CLASS   = "strategy_class";
    private static final String STRATEGY_OPTIONS = "strategy_options";

    private final String name;
    private final boolean durableWrites;
    private final Map<String, String> replication = new HashMap<String, String>();
    private final Map<String, TableMetadata> tables = new ConcurrentHashMap<String, TableMetadata>();

    private KeyspaceMetadata(String name, boolean durableWrites) {
        this.name = name;
        this.durableWrites = durableWrites;
    }

    static KeyspaceMetadata build(Row row) {

        String name = row.getString(KS_NAME);
        boolean durableWrites = row.getBool(DURABLE_WRITES);
        KeyspaceMetadata ksm = new KeyspaceMetadata(name, durableWrites);
        ksm.replication.put("class", row.getString(STRATEGY_CLASS));
        ksm.replication.putAll(TableMetadata.fromJsonMap(row.getString(STRATEGY_OPTIONS)));
        return ksm;
    }

    /**
     * Returns the name of this keyspace.
     *
     * @return the name of this CQL keyspace.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns whether durable writes are set on this keyspace.
     *
     * @return {@code true} if durable writes are set on this keyspace (the
     * default), {@code false} otherwise.
     */
    public boolean isDurableWrites() {
        return durableWrites;
    }

    /**
     * Returns the replication options for this keyspace.
     *
     * @return a map containing the replication options for this keyspace.
     */
    public Map<String, String> getReplication() {
        return Collections.<String, String>unmodifiableMap(replication);
    }

    /**
     * Returns the metadata for a table contained in this keyspace.
     *
     * @param name the name of table to retrieve
     * @return the metadata for table {@code name} in this keyspace if it
     * exists, {@code false} otherwise.
     */
    public TableMetadata getTable(String name) {
        return tables.get(name);
    }

    /**
     * Returns the tables defined in this keyspace.
     *
     * @return a collection of the metadata for the tables defined in this
     * keyspace.
     */
    public Collection<TableMetadata> getTables() {
        return Collections.<TableMetadata>unmodifiableCollection(tables.values());
    }

    /**
     * Returns a {@code String} containing CQL queries representing this
     * keyspace and the table it contains.
     *
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this keyspace, along with all its table.
     *
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     *
     * @return the CQL queries representing this keyspace schema as a {code
     * String}.
     */
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(asCQLQuery()).append("\n");

        for (TableMetadata tm : tables.values())
            sb.append("\n").append(tm.exportAsString()).append("\n");

        return sb.toString();
    }

    /**
     * Returns a CQL query representing this keyspace.
     *
     * This method returns a single 'CREATE KEYSPACE' query with the options
     * corresponding to this keyspace definition.
     *
     * @return the 'CREATE KEYSPACE' query corresponding to this keyspace.
     * @see #exportAsString
     */
    public String asCQLQuery() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE KEYSPACE ").append(name).append(" WITH ");
        sb.append("REPLICATION = { 'class' : '").append(replication.get("class")).append("'");
        for (Map.Entry<String, String> entry : replication.entrySet()) {
            if (entry.getKey().equals("class"))
                continue;
            sb.append(", '").append(entry.getKey()).append("': '").append(entry.getValue()).append("'");
        }
        sb.append(" } AND DURABLE_WRITES = ").append(durableWrites);
        sb.append(";");
        return sb.toString();
    }

    void add(TableMetadata tm) {
        tables.put(tm.getName(), tm);
    }
}
