package com.datastax.driver.core;

import java.util.*;

/**
 * Describes the keyspace defined in the cluster, i.e. the current schema.
 */
public class KeyspaceMetadata {

    public static final String KS_NAME           = "keyspace_name";
    private static final String DURABLE_WRITES   = "durable_writes";
    private static final String STRATEGY_CLASS   = "strategy_class";
    private static final String STRATEGY_OPTIONS = "strategy_options";

    private final String name;
    private final boolean durableWrites;
    private final Map<String, String> replication = new HashMap<String, String>();
    private final Map<String, TableMetadata> tables = new HashMap<String, TableMetadata>();

    private KeyspaceMetadata(String name, boolean durableWrites) {
        this.name = name;
        this.durableWrites = durableWrites;
    }

    static KeyspaceMetadata build(CQLRow row) {

        String name = row.getString(KS_NAME);
        boolean durableWrites = row.getBool(DURABLE_WRITES);
        KeyspaceMetadata ksm = new KeyspaceMetadata(name, durableWrites);
        ksm.replication.put("class", row.getString(STRATEGY_CLASS));
        ksm.replication.put("options", row.getString(STRATEGY_OPTIONS));
        return ksm;
    }

    public String getName() {
        return name;
    }

    public boolean isDurableWrites() {
        return durableWrites;
    }

    public Map<String, String> getReplicationStrategy() {
        return new HashMap<String, String>(replication);
    }

    public TableMetadata getTable(String name) {
        return tables.get(name);
    }

    public Collection<TableMetadata> getTables() {
        return tables.values();
    }

    // TODO: Returning a multi-line string from toString might not be a good idea
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE KEYSPACE ").append(name).append(" WITH ");
        sb.append("STRATEGY_CLASS = ").append(replication.get("class"));
        // TODO: handle the options
        sb.append(";\n");

        for (TableMetadata tm : tables.values())
            sb.append("\n").append(tm);

        return sb.toString();
    }

    void add(TableMetadata tm) {
        tables.put(tm.getName(), tm);
    }
}
