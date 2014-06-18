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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private final ReplicationStrategy strategy;
    private final Map<String, String> replication;

    // TODO: I don't think we change those, so there is probably no need for ConcurrentHashMap. Check if that's the case.
    private final Map<String, TableMetadata> tables = new ConcurrentHashMap<String, TableMetadata>();
    private final Map<String, UDTDefinition> userTypes = new ConcurrentHashMap<String, UDTDefinition>();

    private KeyspaceMetadata(String name, boolean durableWrites, Map<String, String> replication) {
        this.name = name;
        this.durableWrites = durableWrites;
        this.replication = replication;
        this.strategy = ReplicationStrategy.create(replication);
    }

    static KeyspaceMetadata build(int protocolVersion, Row row, List<Row> udtRows) {

        String name = row.getString(KS_NAME);
        boolean durableWrites = row.getBool(DURABLE_WRITES);

        Map<String, String> replicationOptions = new HashMap<String, String>();
        replicationOptions.put("class", row.getString(STRATEGY_CLASS));
        replicationOptions.putAll(SimpleJSONParser.parseStringMap(row.getString(STRATEGY_OPTIONS)));

        KeyspaceMetadata ksm = new KeyspaceMetadata(name, durableWrites, replicationOptions);

        if (udtRows == null)
            return ksm;

        for (Row r : udtRows) {
            UDTDefinition def = UDTDefinition.build(protocolVersion, r);
            ksm.userTypes.put(def.getName(), def);
        }

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
     * @return the metadata for table {@code name} if it exists in this keyspace,
     * {@code null} otherwise.
     */
    public TableMetadata getTable(String name) {
        return tables.get(Metadata.handleId(name));
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
     * Returns the definition for a user defined type (UDT) in this keyspace.
     *
     * @param name the name of UDT definition to retrieve
     * @return the definition for {@code name} if it exists in this keyspace,
     * {@code null} otherwise.
     */
    public UDTDefinition getUserType(String name) {
        return userTypes.get(Metadata.handleId(name));
    }

    /**
     * Returns the user types defined in this keyspace.
     *
     * @return a collection of the definition for the user types defined in this
     * keyspace.
     */
    public Collection<UDTDefinition> getUserTypes() {
        return Collections.<UDTDefinition>unmodifiableCollection(userTypes.values());
    }

    /**
     * Returns a {@code String} containing CQL queries representing this
     * keyspace and the user types and tables it contains.
     * <p>
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this keyspace, along with all its user
     * types/tables.
     * <p>
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     *
     * @return the CQL queries representing this keyspace schema as a {code
     * String}.
     */
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(asCQLQuery()).append('\n');

        for (UDTDefinition udt : userTypes.values())
            sb.append('\n').append(udt.exportAsString()).append('\n');

        for (TableMetadata tm : tables.values())
            sb.append('\n').append(tm.exportAsString()).append('\n');

        return sb.toString();
    }

    /**
     * Returns a CQL query representing this keyspace.
     * <p>
     * This method returns a single 'CREATE KEYSPACE' query with the options
     * corresponding to this keyspace definition.
     *
     * @return the 'CREATE KEYSPACE' query corresponding to this keyspace.
     * @see #exportAsString
     */
    public String asCQLQuery() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE KEYSPACE ").append(Metadata.escapeId(name)).append(" WITH ");
        sb.append("REPLICATION = { 'class' : '").append(replication.get("class")).append('\'');
        for (Map.Entry<String, String> entry : replication.entrySet()) {
            if (entry.getKey().equals("class"))
                continue;
            sb.append(", '").append(entry.getKey()).append("': '").append(entry.getValue()).append('\'');
        }
        sb.append(" } AND DURABLE_WRITES = ").append(durableWrites);
        sb.append(';');
        return sb.toString();
    }

    @Override
    public String toString() {
        return asCQLQuery();
    }

    void add(TableMetadata tm) {
        tables.put(tm.getName(), tm);
    }

    ReplicationStrategy replicationStrategy() {
        return strategy;
    }
}
