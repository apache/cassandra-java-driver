package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 */
public class ClusterMetadata {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadata.class);

    private final Cluster.Manager cluster;
    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();
    private final ConcurrentMap<String, KeyspaceMetadata> keyspaces = new ConcurrentHashMap<String, KeyspaceMetadata>();

    ClusterMetadata(Cluster.Manager cluster) {
        this.cluster = cluster;
    }

    // Synchronized to make it easy to detect dropped keyspaces
    synchronized void rebuildSchema(String keyspace, String table, ResultSet ks, ResultSet cfs, ResultSet cols) {

        Map<String, List<CQLRow>> cfDefs = new HashMap<String, List<CQLRow>>();
        Map<String, Map<String, List<CQLRow>>> colsDefs = new HashMap<String, Map<String, List<CQLRow>>>();

        // Gather cf defs
        for (CQLRow row : cfs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            List<CQLRow> l = cfDefs.get(ksName);
            if (l == null) {
                l = new ArrayList<CQLRow>();
                cfDefs.put(ksName, l);
            }
            l.add(row);
        }

        // Gather columns per Cf
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

        if (table == null) {
            assert ks != null;
            Set<String> addedKs = new HashSet<String>();
            for (CQLRow ksRow : ks) {
                String ksName = ksRow.getString(KeyspaceMetadata.KS_NAME);
                KeyspaceMetadata ksm = KeyspaceMetadata.build(ksRow);

                if (cfDefs.containsKey(ksName)) {
                    buildTableMetadata(ksm, cfDefs.get(ksName), colsDefs.get(ksName));
                }
                addedKs.add(ksName);
                keyspaces.put(ksName, ksm);
            }

            // If keyspace is null, it means we're rebuilding from scratch, so
            // remove anything that was not just added as it means it's a dropped keyspace
            if (keyspace == null) {
                Iterator<String> iter = keyspaces.keySet().iterator();
                while (iter.hasNext()) {
                    if (!addedKs.contains(iter.next()))
                        iter.remove();
                }
            }
        } else {
            assert keyspace != null;
            KeyspaceMetadata ksm = keyspaces.get(keyspace);

            // If we update a keyspace we don't know about, something went
            // wrong. Log an error an schedule a full schema rebuilt.
            if (ksm == null) {
                logger.error(String.format("Asked to rebuild table %s.%s but I don't know keyspace %s", keyspace, table, keyspace));
                cluster.submitSchemaRefresh(null, null);
                return;
            }

            if (cfDefs.containsKey(keyspace))
                buildTableMetadata(ksm, cfDefs.get(keyspace), colsDefs.get(keyspace));
        }
    }

    private static void buildTableMetadata(KeyspaceMetadata ksm, List<CQLRow> cfRows, Map<String, List<CQLRow>> colsDefs) {
        for (CQLRow cfRow : cfRows) {
            String cfName = cfRow.getString(TableMetadata.CF_NAME);
            TableMetadata tm = TableMetadata.build(ksm, cfRow);

            if (colsDefs == null || colsDefs.get(cfName) == null)
                continue;

            for (CQLRow colRow : colsDefs.get(cfName)) {
                ColumnMetadata cm = ColumnMetadata.build(tm, colRow);
            }
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

    // For internal use only
    Collection<Host> allHosts() {
        return hosts.values();
    }

    /**
     * Returns the known hosts of this cluster.
     *
     * @return A set will all the know host of this cluster.
     */
    public Set<Host> getAllHosts() {
        return new HashSet<Host>(allHosts());
    }

    /**
     * Return the metadata of a keyspace given its name.
     *
     * @param keyspace the name of the keyspace for which metadata should be
     * returned.
     * @return the metadat of the requested keyspace or {@code null} if {@code
     * keyspace} is not a known keyspace.
     */
    public KeyspaceMetadata getKeyspace(String keyspace) {
        return keyspaces.get(keyspace);
    }

    /**
     * Returns a list of all the defined keyspaces.
     *
     * @return a list of all the defined keyspaces.
     */
    public List<KeyspaceMetadata> getKeyspaces() {
        return new ArrayList<KeyspaceMetadata>(keyspaces.values());
    }

    /**
     * Return a {@code String} containing CQL queries representing the schema
     * of this cluster.
     *
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this cluster.
     *
     * Note that the returned String is formatted to be human readable (for
     * some defintion of human readable at least).
     *
     * @return the CQL queries representing this cluster schema as a {code
     * String}.
     */
    // TODO: add some boolean arg to deal with thift defs that can't be fully
    // represented by CQL queries (like either throw an exception or
    // do-our-best). Or some other way to deal with that.
    public String exportSchemaAsString() {
        StringBuilder sb = new StringBuilder();

        for (KeyspaceMetadata ksm : keyspaces.values())
            sb.append(ksm.exportAsString()).append("\n");

        return sb.toString();
    }
}
