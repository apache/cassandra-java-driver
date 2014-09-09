/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 */
public class Metadata {

    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);

    private final Cluster.Manager cluster;
    volatile String clusterName;
    volatile String partitioner;
    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();
    private final ConcurrentMap<String, KeyspaceMetadata> keyspaces = new ConcurrentHashMap<String, KeyspaceMetadata>();
    volatile TokenMap tokenMap;

    private static final Pattern cqlId = Pattern.compile("\\w+");
    private static final Pattern lowercaseId = Pattern.compile("[a-z][a-z0-9_]*");

    Metadata(Cluster.Manager cluster) {
        this.cluster = cluster;
    }

    // Synchronized to make it easy to detect dropped keyspaces
    synchronized void rebuildSchema(String keyspace, String table, ResultSet ks, ResultSet udts, ResultSet cfs, ResultSet cols, VersionNumber cassandraVersion) {

        Map<String, List<Row>> cfDefs = new HashMap<String, List<Row>>();
        Map<String, List<Row>> udtDefs = new HashMap<String, List<Row>>();
        Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs = new HashMap<String, Map<String, Map<String, ColumnMetadata.Raw>>>();

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

        // Gather udt defs
        if (udts != null) {
            for (Row row : udts) {
                String ksName = row.getString(KeyspaceMetadata.KS_NAME);
                List<Row> l = udtDefs.get(ksName);
                if (l == null) {
                    l = new ArrayList<Row>();
                    udtDefs.put(ksName, l);
                }
                l.add(row);
            }
        }

        // Gather columns per Cf
        for (Row row : cols) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            String cfName = row.getString(TableMetadata.CF_NAME);
            Map<String, Map<String, ColumnMetadata.Raw>> colsByCf = colsDefs.get(ksName);
            if (colsByCf == null) {
                colsByCf = new HashMap<String, Map<String, ColumnMetadata.Raw>>();
                colsDefs.put(ksName, colsByCf);
            }
            Map<String, ColumnMetadata.Raw> l = colsByCf.get(cfName);
            if (l == null) {
                l = new HashMap<String, ColumnMetadata.Raw>();
                colsByCf.put(cfName, l);
            }
            ColumnMetadata.Raw c = ColumnMetadata.Raw.fromRow(row, cassandraVersion);
            l.put(c.name, c);
        }

        if (table == null) {
            assert ks != null;
            Set<String> addedKs = new HashSet<String>();
            for (Row ksRow : ks) {
                String ksName = ksRow.getString(KeyspaceMetadata.KS_NAME);
                KeyspaceMetadata ksm = KeyspaceMetadata.build(ksRow, udtDefs.get(ksName));

                if (cfDefs.containsKey(ksName)) {
                    buildTableMetadata(ksm, cfDefs.get(ksName), colsDefs.get(ksName), cassandraVersion);
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
                buildTableMetadata(ksm, cfDefs.get(keyspace), colsDefs.get(keyspace), cassandraVersion);
        }
    }

    private void buildTableMetadata(KeyspaceMetadata ksm, List<Row> cfRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, VersionNumber cassandraVersion) {
        for (Row cfRow : cfRows) {
            String cfName = cfRow.getString(TableMetadata.CF_NAME);
            try {
                Map<String, ColumnMetadata.Raw> cols = colsDefs == null ? null : colsDefs.get(cfName);
                if (cols == null || cols.isEmpty()) {
                    if (cassandraVersion.getMajor() >= 2) {
                        // In C* >= 2.0, we should never have no columns metadata because at the very least we should
                        // have the metadata corresponding to the default CQL metadata. So if we don't have any columns,
                        // that can only mean that the table got creating concurrently with our schema queries, and the
                        // query for columns metadata reached the node before the table was persisted while the table
                        // metadata one reached it afterwards. We could make the query to the column metadata sequential
                        // with the table metadata instead of in parallel, but it's probably not worth making it slower
                        // all the time to avoid this race since 1) it's very very uncommon and 2) we can just ignore the
                        // incomplete table here for now and it'll get updated next time with no particular consequence
                        // (if the table creation was concurrent with our querying, we'll get a notifciation later and
                        // will reupdate the schema for it anyway). See JAVA-320 for why we need this.
                        continue;
                    } else {
                        // C* 1.2 don't persists default CQL metadata, so it's possible not to have columns (for thirft
                        // tables). But in that case TableMetadata.build() knows how to handle it.
                        cols = Collections.<String, ColumnMetadata.Raw>emptyMap();
                    }
                }
                TableMetadata.build(ksm, cfRow, cols, cassandraVersion);
            } catch (RuntimeException e) {
                // See ControlConnection#refreshSchema for why we'd rather not probably this further
                logger.error(String.format("Error parsing schema for table %s.%s: "
                                           + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\") will be missing or incomplete",
                                           ksm.getName(), cfName, ksm.getName(), cfName), e);
            }
        }
    }

    synchronized void rebuildTokenMap(String partitioner, Map<Host, Collection<String>> allTokens) {
        if (allTokens.isEmpty())
            return;

        Token.Factory factory = partitioner == null
                              ? (tokenMap == null ? null : tokenMap.factory)
                              : Token.getFactory(partitioner);
        if (factory == null)
            return;

        this.tokenMap = TokenMap.build(factory, allTokens, keyspaces.values());
    }

    Host add(InetSocketAddress address) {
        Host newHost = new Host(address, cluster.convictionPolicyFactory);
        Host previous = hosts.putIfAbsent(address, newHost);
        return previous == null ? newHost : null;
    }

    boolean remove(Host host) {
        return hosts.remove(host.getSocketAddress()) != null;
    }

    Host getHost(InetSocketAddress address) {
        return hosts.get(address);
    }

    // For internal use only
    Collection<Host> allHosts() {
        return hosts.values();
    }

    // Deal with case sensitivity for a given keyspace or table id
    static String handleId(String id) {
        // Shouldn't really happen for this method, but no reason to fail here
        if (id == null)
            return null;

        if (cqlId.matcher(id).matches())
            return id.toLowerCase();

        // Check if it's enclosed in quotes. If it is, remove them
        if (id.charAt(0) == '"' && id.charAt(id.length() - 1) == '"')
            return id.substring(1, id.length() - 1);

        // otherwise, just return the id.
        return id;
    }

    // Escape a CQL3 identifier based on its value as read from the schema
    // tables. Because it comes from Cassandra, we could just always quote it,
    // but to get a nicer output we don't do it if it's not necessary.
    static String escapeId(String ident) {
        // we don't need to escape if it's lowercase and match non-quoted CQL3 ids.
        return lowercaseId.matcher(ident).matches() ? ident : quote(ident);
    }

    /**
     * Quote a keyspace, table or column identifier to make it case sensitive.
     * <p>
     * CQL identifiers, including keyspace, table and column ones, are case insensitive
     * by default. Case sensitive identifiers can however be provided by enclosing
     * the identifier in double quotes (see the
     * <a href="http://cassandra.apache.org/doc/cql3/CQL.html#identifiers">CQL documentation</a>
     * for details). If you are using case sensitive identifiers, this method
     * can be used to enclose such identifier in double quotes, making it case
     * sensitive.
     *
     * @param id the keyspace or table identifier.
     * @return {@code id} enclosed in double-quotes, for use in methods like
     * {@link #getReplicas}, {@link #getKeyspace}, {@link KeyspaceMetadata#getTable}
     * or even {@link Cluster#connect(String)}.
     */
    public static String quote(String id) {
        return '"' + id + '"';
    }

    /**
     * Returns the set of hosts that are replica for a given partition key.
     * <p>
     * Note that this method is a best effort method. Consumers should not rely
     * too heavily on the result of this method not being stale (or even empty).
     *
     * @param keyspace the name of the keyspace to get replicas for.
     * @param partitionKey the partition key for which to find the set of
     * replica.
     * @return the (immutable) set of replicas for {@code partitionKey} as know
     * by the driver. No strong guarantee is provided on the stalelessness of
     * this information. It is also not guarantee that the returned set won't
     * be empty (which is then some form of staleness).
     */
    public Set<Host> getReplicas(String keyspace, ByteBuffer partitionKey) {
        keyspace = handleId(keyspace);
        TokenMap current = tokenMap;
        if (current == null) {
            return Collections.emptySet();
        } else {
            Set<Host> hosts = current.getReplicas(keyspace, current.factory.hash(partitionKey));
            return hosts == null ? Collections.<Host>emptySet() : hosts;
        }
    }

    /**
     * The Cassandra name for the cluster connect to.
     *
     * @return the Cassandra name for the cluster connect to.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * The partitioner in use as reported by the Cassandra nodes.
     *
     * @return the partitioner in use as reported by the Cassandra nodes.
     */
    public String getPartitioner() {
        return partitioner;
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
        return keyspaces.get(handleId(keyspace));
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
            sb.append(ksm.exportAsString()).append('\n');

        return sb.toString();
    }

    static class TokenMap {

        private final Token.Factory factory;
        private final Map<String, Map<Token, Set<Host>>> tokenToHosts;
        private final List<Token> ring;
        final Set<Host> hosts;

        private TokenMap(Token.Factory factory, Map<String, Map<Token, Set<Host>>> tokenToHosts, List<Token> ring, Set<Host> hosts) {
            this.factory = factory;
            this.tokenToHosts = tokenToHosts;
            this.ring = ring;
            this.hosts = hosts;
        }

        public static TokenMap build(Token.Factory factory, Map<Host, Collection<String>> allTokens, Collection<KeyspaceMetadata> keyspaces) {

            Set<Host> hosts = allTokens.keySet();
            Map<Token, Host> tokenToPrimary = new HashMap<Token, Host>();
            Set<Token> allSorted = new TreeSet<Token>();

            for (Map.Entry<Host, Collection<String>> entry : allTokens.entrySet()) {
                Host host = entry.getKey();
                for (String tokenStr : entry.getValue()) {
                    try {
                        Token t = factory.fromString(tokenStr);
                        allSorted.add(t);
                        tokenToPrimary.put(t, host);
                    } catch (IllegalArgumentException e) {
                        // If we failed parsing that token, skip it
                    }
                }
            }

            List<Token> ring = new ArrayList<Token>(allSorted);

            Map<String, Map<Token, Set<Host>>> tokenToHosts = new HashMap<String, Map<Token, Set<Host>>>();
            for (KeyspaceMetadata keyspace : keyspaces)
            {
                ReplicationStrategy strategy = keyspace.replicationStrategy();
                if (strategy == null) {
                    tokenToHosts.put(keyspace.getName(), makeNonReplicatedMap(tokenToPrimary));
                } else {
                    tokenToHosts.put(keyspace.getName(), strategy.computeTokenToReplicaMap(tokenToPrimary, ring));
                }
            }
            return new TokenMap(factory, tokenToHosts, ring, hosts);
        }

        private Set<Host> getReplicas(String keyspace, Token token) {

            Map<Token, Set<Host>> keyspaceHosts = tokenToHosts.get(keyspace);
            if (keyspaceHosts == null)
                return Collections.emptySet();

            // Find the primary replica
            int i = Collections.binarySearch(ring, token);
            if (i < 0) {
                i = -i - 1;
                if (i >= ring.size())
                    i = 0;
            }

            return keyspaceHosts.get(ring.get(i));
        }

        private static Map<Token, Set<Host>> makeNonReplicatedMap(Map<Token, Host> input) {
            Map<Token, Set<Host>> output = new HashMap<Token, Set<Host>>(input.size());
            for (Map.Entry<Token, Host> entry : input.entrySet())
                output.put(entry.getKey(), ImmutableSet.of(entry.getValue()));
            return output;
        }
    }
}
