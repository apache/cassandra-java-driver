package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 */
public class ClusterMetadata {

    private final Cluster.Manager cluster;
    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();
    private final Map<String, KeyspaceMetadata> keyspaces = new HashMap<String, KeyspaceMetadata>();

    ClusterMetadata(Cluster.Manager cluster) {
        this.cluster = cluster;
    }

    void rebuildSchema(ResultSet ks, ResultSet cfs, ResultSet cols) {

        // TODO: we need to switch the keyspaces map completely

        Map<String, List<CQLRow>> cfDefs = new HashMap<String, List<CQLRow>>();
        Map<String, Map<String, List<CQLRow>>> colsDefs = new HashMap<String, Map<String, List<CQLRow>>>();

        for (CQLRow row : cfs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            List<CQLRow> l = cfDefs.get(ksName);
            if (l == null) {
                l = new ArrayList<CQLRow>();
                cfDefs.put(ksName, l);
            }
            l.add(row);
        }

        for (CQLRow row : cols) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            String cfName = row.getString(TableMetadata.CF_NAME);
            Map<String, List<CQLRow>> colsByCf = colsDefs.get(ksName);
            if (colsByCf == null) {
                colsByCf = new HashMap<String, List<CQLRow>>();
                colsDefs.put(ksName, colsByCf);
            }
            List<CQLRow> l = colsByCf.get(cfName);
            if (l == null) {
                l = new ArrayList<CQLRow>();
                colsByCf.put(cfName, l);
            }
            l.add(row);
        }

        for (CQLRow ksRow : ks) {
            String ksName = ksRow.getString(KeyspaceMetadata.KS_NAME);
            KeyspaceMetadata ksm = KeyspaceMetadata.build(ksRow);

            if (cfDefs.get(ksName) != null) {
                for (CQLRow cfRow : cfDefs.get(ksName)) {
                    String cfName = cfRow.getString(TableMetadata.CF_NAME);
                    TableMetadata tm = TableMetadata.build(ksm, cfRow);

                    if (colsDefs.get(ksName) == null || colsDefs.get(ksName).get(cfName) == null)
                        continue;

                    for (CQLRow colRow : colsDefs.get(ksName).get(cfName)) {
                        ColumnMetadata cm = ColumnMetadata.build(tm, colRow);
                    }
                }
            }

            keyspaces.put(ksName, ksm);
        }
    }

    Host add(InetSocketAddress address) {
        Host newHost = new Host(address, cluster.convictionPolicyFactory);
        Host previous = hosts.putIfAbsent(address, newHost);
        if (previous == null)
        {
            newHost.monitor().register(cluster);
            return newHost;
        }
        else
        {
            return null;
        }
    }

    boolean remove(Host host) {
        return hosts.remove(host.getAddress()) != null;
    }

    Host getHost(InetSocketAddress address) {
        return hosts.get(address);
    }

    Collection<Host> allHosts() {
        return hosts.values();
    }

    public KeyspaceMetadata getKeyspace(String keyspace) {
        return keyspaces.get(keyspace);
    }

    // TODO: Returning a multi-line string from toString might not be a good idea
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (KeyspaceMetadata ksm : keyspaces.values())
            sb.append(ksm).append("\n");

        return sb.toString();
    }
}
