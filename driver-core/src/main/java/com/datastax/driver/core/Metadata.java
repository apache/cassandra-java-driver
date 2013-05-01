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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 */
public class Metadata {

    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);

    private final Cluster.Manager cluster;
    volatile String clusterName;
    private final ConcurrentMap<InetAddress, Host> hosts = new ConcurrentHashMap<InetAddress, Host>();
    private final ConcurrentMap<String, KeyspaceMetadata> keyspaces = new ConcurrentHashMap<String, KeyspaceMetadata>();

    private volatile TokenMap<? extends Token> tokenMap;

    Metadata(Cluster.Manager cluster) {
        this.cluster = cluster;
    }

    // Synchronized to make it easy to detect dropped keyspaces
    synchronized void rebuildSchema(String keyspace, String table, ResultSet ks, ResultSet cfs, ResultSet cols) {

        Map<String, List<Row>> cfDefs = new HashMap<String, List<Row>>();
        Map<String, Map<String, List<Row>>> colsDefs = new HashMap<String, Map<String, List<Row>>>();

        // Gather cf defs
        for (Row row : cfs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            List<Row> l = cfDefs.get(ksName);
            if (l == null) {
                l = new ArrayList<Row>();
                cfDefs.put(ksName, l);
            }
            l.add(row);
        }

        // Gather columns per Cf
        for (Row row : cols) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            String cfName = row.getString(TableMetadata.CF_NAME);
            Map<String, List<Row>> colsByCf = colsDefs.get(ksName);
            if (colsByCf == null) {
                colsByCf = new HashMap<String, List<Row>>();
                colsDefs.put(ksName, colsByCf);
            }
            List<Row> l = colsByCf.get(cfName);
            if (l == null) {
                l = new ArrayList<Row>();
                colsByCf.put(cfName, l);
            }
            l.add(row);
        }

        if (table == null) {
            assert ks != null;
            Set<String> addedKs = new HashSet<String>();
            for (Row ksRow : ks) {
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

    private static void buildTableMetadata(KeyspaceMetadata ksm, List<Row> cfRows, Map<String, List<Row>> colsDefs) {
        boolean hasColumns = (colsDefs != null) && !colsDefs.isEmpty();
        for (Row cfRow : cfRows) {
            String cfName = cfRow.getString(TableMetadata.CF_NAME);
            TableMetadata tm = TableMetadata.build(ksm, cfRow, hasColumns);

            if (!hasColumns || colsDefs.get(cfName) == null)
                continue;

            for (Row colRow : colsDefs.get(cfName)) {
                ColumnMetadata.build(tm, colRow);
            }
        }
    }

    synchronized void rebuildTokenMap(String partitioner, Map<Host, Collection<String>> allTokens) {
        this.tokenMap = TokenMap.build(partitioner, allTokens);
    }

    Host add(InetAddress address) {
        Host newHost = new Host(address, cluster.convictionPolicyFactory);
        Host previous = hosts.putIfAbsent(address, newHost);
        if (previous == null)
        {
            newHost.getMonitor().register(cluster);
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

    Host getHost(InetAddress address) {
        return hosts.get(address);
    }

    // For internal use only
    Collection<Host> allHosts() {
        return hosts.values();
    }

    /**
     * Returns the set of hosts that are replica for a given partition key.
     * <p>
     * Note that this method is a best effort method. Consumers should not rely
     * too heavily on the result of this method not being stale (or even empty).
     *
     * @param partitionKey the partition key for which to find the set of
     * replica.
     * @return the (immutable) set of replicas for {@code partitionKey} as know
     * by the driver. No strong guarantee is provided on the stalelessness of
     * this information. It is also not guarantee that the returned set won't
     * be empty (which is then some form of staleness).
     */
    public Set<Host> getReplicas(ByteBuffer partitionKey) {
        TokenMap current = tokenMap;
        if (current == null) {
            return Collections.emptySet();
        } else {
            return current.getReplicas(current.factory.hash(partitionKey));
        }
    }

    /**
     * Returns the Cassandra name for the cluster connect to.
     *
     * @return the Cassandra name for the cluster connect to.
     */
    public String getClusterName() {
        return clusterName;
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
     * Returns the metadata of a keyspace given its name.
     *
     * @param keyspace the name of the keyspace for which metadata should be
     * returned.
     * @return the metadata of the requested keyspace or {@code null} if {@code
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
     * Returns a {@code String} containing CQL queries representing the schema
     * of this cluster.
     *
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this cluster.
     *
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     *
     * @return the CQL queries representing this cluster schema as a {code
     * String}.
     */
    public String exportSchemaAsString() {
        StringBuilder sb = new StringBuilder();

        for (KeyspaceMetadata ksm : keyspaces.values())
            sb.append(ksm.exportAsString()).append("\n");

        return sb.toString();
    }

    static class TokenMap<T extends Token<T>> {

        private final Token.Factory<T> factory;
        private final Map<Token<T>, Set<Host>> tokenToHosts;
        private final List<Token<T>> ring;

        private TokenMap(Token.Factory<T> factory, Map<Token<T>, Set<Host>> tokenToHosts, List<Token<T>> ring) {
            this.factory = factory;
            this.tokenToHosts = tokenToHosts;
            this.ring = ring;
        }

        public static <T extends Token<T>> TokenMap<T> build(String partitioner, Map<Host, Collection<String>> allTokens) {

            Token.Factory<T> factory = (Token.Factory<T>)Token.getFactory(partitioner);
            if (factory == null)
                return null;

            Map<Token<T>, Set<Host>> tokenToHosts = new HashMap<Token<T>, Set<Host>>();
            Set<Token<T>> allSorted = new TreeSet<Token<T>>();

            for (Map.Entry<Host, Collection<String>> entry : allTokens.entrySet()) {
                Host host = entry.getKey();
                for (String tokenStr : entry.getValue()) {
                    try {
                        Token<T> t = factory.fromString(tokenStr);
                        allSorted.add(t);
                        Set<Host> hosts = tokenToHosts.get(t);
                        if (hosts == null) {
                            hosts = new HashSet<Host>();
                            tokenToHosts.put(t, hosts);
                        }
                        hosts.add(host);
                    } catch (IllegalArgumentException e) {
                        // If we failed parsing that token, skip it
                    }
                }
            }
            // Make all the inet set immutable so we can share them publicly safely
            for (Map.Entry<Token<T>, Set<Host>> entry: tokenToHosts.entrySet()) {
                entry.setValue(Collections.unmodifiableSet(entry.getValue()));
            }
            return new TokenMap(factory, tokenToHosts, new ArrayList<Token<T>>(allSorted));
        }

        private Set<Host> getReplicas(T token) {

            // Find the primary replica
            int i = Collections.binarySearch(ring, token);
            if (i < 0) {
                i = (i + 1) * (-1);
                if (i >= ring.size())
                    i = 0;
            }

            return tokenToHosts.get(ring.get(i));
        }
    }
}
