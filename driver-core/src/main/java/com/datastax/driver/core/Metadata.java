/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

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
    synchronized void rebuildSchema(String keyspaceToRebuild, String tableToRebuild, ResultSet ks, ResultSet cfs, ResultSet cols, VersionNumber cassandraVersion) {
        Map<String, List<Row>> tableRows = groupByKeyspace(cfs);
        Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs = groupByKeyspaceAndTable(cols, cassandraVersion);
        if (tableToRebuild == null) {
            // building a keyspace (keyspaceToRebuild != null), or the whole schema (keyspaceToRebuild == null)
            assert ks != null;
            Map<String, KeyspaceMetadata> keyspaces = buildKeyspaces(ks, tableRows, colsDefs, cassandraVersion);
            updateKeyspaces(this.keyspaces, keyspaces, keyspaceToRebuild);
        } else {
            // building only a table
            assert keyspaceToRebuild != null;
            KeyspaceMetadata keyspace = this.keyspaces.get(keyspaceToRebuild);
            // If we update a keyspace we don't know about, something went
            // wrong. Log an error and schedule a full schema rebuild.
            if (keyspace == null) {
                logger.info(String.format("Asked to rebuild table %s.%s but I don't know keyspace %s", keyspaceToRebuild, tableToRebuild, keyspaceToRebuild));
                cluster.submitSchemaRefresh(null, null);
                return;
            }
            if (tableRows.containsKey(keyspaceToRebuild)) {
                Map<String, TableMetadata> tables = buildTables(keyspace, tableRows.get(keyspaceToRebuild), colsDefs.get(keyspaceToRebuild), cassandraVersion);
                updateTables(keyspace.tables, tables, tableToRebuild);
            }
        }
    }

    private Map<String, List<Row>> groupByKeyspace(ResultSet cfs) {
        // Gather cf defs
        Map<String, List<Row>> cfDefs = new HashMap<String, List<Row>>();
        for (Row row : cfs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            List<Row> l = cfDefs.get(ksName);
            if (l == null) {
                l = new ArrayList<Row>();
                cfDefs.put(ksName, l);
            }
            l.add(row);
        }
        return cfDefs;
    }

    private Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> groupByKeyspaceAndTable(ResultSet cols, VersionNumber cassandraVersion) {
        Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs = new HashMap<String, Map<String, Map<String, ColumnMetadata.Raw>>>();
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
        return colsDefs;
    }

    private Map<String, KeyspaceMetadata> buildKeyspaces(ResultSet keyspaceRows, Map<String, List<Row>> tableRows, Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs, VersionNumber cassandraVersion) {
        Map<String, KeyspaceMetadata> keyspaces = new LinkedHashMap<String, KeyspaceMetadata>();
        for (Row keyspaceRow : keyspaceRows) {
            KeyspaceMetadata keyspace = KeyspaceMetadata.build(keyspaceRow);
            Map<String, TableMetadata> tables = buildTables(keyspace, tableRows.get(keyspace.getName()), colsDefs.get(keyspace.getName()), cassandraVersion);
            for (TableMetadata table : tables.values()) {
                keyspace.add(table);
            }
            keyspaces.put(keyspace.getName(), keyspace);
        }
        return keyspaces;
    }

    private Map<String, TableMetadata> buildTables(KeyspaceMetadata keyspace, List<Row> tableRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, VersionNumber cassandraVersion) {
        Map<String, TableMetadata> tables = new LinkedHashMap<String, TableMetadata>();
        if(tableRows != null) {
            for (Row tableDef : tableRows) {
                String cfName = tableDef.getString(TableMetadata.CF_NAME);
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
                            cols = Collections.emptyMap();
                        }
                    }
                    TableMetadata table = TableMetadata.build(keyspace, tableDef, cols, cassandraVersion);
                    tables.put(table.getName(), table);
                } catch (RuntimeException e) {
                    // See ControlConnection#refreshSchema for why we'd rather not probably this further
                    logger.error(String.format("Error parsing schema for table %s.%s: "
                            + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\") will be missing or incomplete",
                        keyspace.getName(), cfName, keyspace.getName(), cfName), e);
                }
            }
        }
        return tables;
    }

    /**
     * Update {@code oldKeyspaces} with the changes contained in {@code newKeyspaces}.
     * This method also takes care of triggering the relevant events
     * as the updates take place.
     *
     * @param oldKeyspaces the set of keyspaces to be updated.
     * @param newKeyspaces the temporary set of keyspaces built with information gathered
     * from schema tables.
     * @param keyspaceToRebuild If we are rebuilding just one keyspace, the update operation will be limited
     * to this keyspace only (in which case {@code newKeyspaces} shoudl contain only one entry for it)
     */
    private void updateKeyspaces(Map<String, KeyspaceMetadata> oldKeyspaces, Map<String, KeyspaceMetadata> newKeyspaces, String keyspaceToRebuild) {
        Iterator<KeyspaceMetadata> it = oldKeyspaces.values().iterator();
        while (it.hasNext()) {
            KeyspaceMetadata oldKeyspace = it.next();
            String keyspaceName = oldKeyspace.getName();
            // If we're rebuilding only a single keyspace, we should only consider that one
            // because newKeyspaces will only contain that keyspace.
            if ((keyspaceToRebuild == null || keyspaceToRebuild.equals(keyspaceName)) && !newKeyspaces.containsKey(keyspaceName)) {
                it.remove();
                triggerOnKeyspaceRemoved(oldKeyspace);
            }
        }
        for (KeyspaceMetadata newKeyspace : newKeyspaces.values()) {
            KeyspaceMetadata oldKeyspace = oldKeyspaces.put(newKeyspace.getName(), newKeyspace);
            if (oldKeyspace == null) {
                triggerOnKeyspaceAdded(newKeyspace);
            } else if (!oldKeyspace.equals(newKeyspace)) {
                triggerOnKeyspaceChanged(newKeyspace, oldKeyspace);
            }
            Map<String, TableMetadata> oldTables = oldKeyspace == null ? new HashMap<String, TableMetadata>() : oldKeyspace.tables;
            Map<String, TableMetadata> newTables = newKeyspace.tables;
            updateTables(oldTables, newTables, null);
        }
    }

    /**
     * Update {@code oldTables} with the changes contained in {@code newTables}.
     * This method also takes care of triggering the relevant events
     * as the updates take place.
     *
     * @param oldTables the set of tables to be updated.
     * @param newTables the temporary set of tables built with information gathered
     * from schema tables.
     * @param tableToRebuild If we are rebuilding just one table, the update operation will be limited
     * to this table only (in which case {@code newTables} shoudl contain only one entry for it)
     */
    private void updateTables(Map<String, TableMetadata> oldTables, Map<String, TableMetadata> newTables, String tableToRebuild) {
        Iterator<TableMetadata> it = oldTables.values().iterator();
        while (it.hasNext()) {
            TableMetadata oldTable = it.next();
            String tableName = oldTable.getName();
            // If we're rebuilding only a single table, we should only consider that one
            // because newTables will only contain that table.
            if ((tableToRebuild == null || tableToRebuild.equals(tableName)) && !newTables.containsKey(tableName)) {
                it.remove();
                triggerOnTableRemoved(oldTable);
            }
        }
        for (TableMetadata newTable : newTables.values()) {
            TableMetadata oldTable = oldTables.put(newTable.getName(), newTable);
            if (oldTable == null) {
                triggerOnTableAdded(newTable);
            } else if (!oldTable.equals(newTable)) {
                triggerOnTableChanged(newTable, oldTable);
            }
        }
    }

    void triggerOnKeyspaceAdded(KeyspaceMetadata keyspace) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onKeyspaceAdded(keyspace);
        }
    }

    void triggerOnKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onKeyspaceChanged(current, previous);
        }
    }

    void triggerOnKeyspaceRemoved(KeyspaceMetadata keyspace) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onKeyspaceRemoved(keyspace);
        }
    }

    void triggerOnTableAdded(TableMetadata table) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onTableAdded(table);
        }
    }

    void triggerOnTableChanged(TableMetadata current, TableMetadata previous) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onTableChanged(current, previous);
        }
    }

    void triggerOnTableRemoved(TableMetadata table) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onTableRemoved(table);
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
        Host newHost = new Host(address, cluster.convictionPolicyFactory, cluster);
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
     * Returns the token ranges that define data distribution in the ring.
     * <p>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale.
     *
     * @return the token ranges.
     */
    public Set<TokenRange> getTokenRanges() {
        TokenMap current = tokenMap;
        return (current == null) ? Collections.<TokenRange>emptySet() : current.tokenRanges;
    }

    /**
     * Returns the token ranges that are replicated on the given host, for the given
     * keyspace.
     * <p>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale (or even empty).
     *
     * @param keyspace the name of the keyspace to get token ranges for.
     * @param host the host.
     * @return the (immutable) set of token ranges for {@code host} as known
     * by the driver.
     */
    public Set<TokenRange> getTokenRanges(String keyspace, Host host) {
        keyspace = handleId(keyspace);
        TokenMap current = tokenMap;
        if (current == null) {
            return Collections.emptySet();
        } else {
            Map<Host, Set<TokenRange>> dcRanges = current.hostsToRanges.get(keyspace);
            if (dcRanges == null) {
                return Collections.emptySet();
            } else {
                Set<TokenRange> ranges = dcRanges.get(host);
                return (ranges == null) ? Collections.<TokenRange>emptySet() : ranges;
            }
        }
    }

    /**
     * Returns the set of hosts that are replica for a given partition key.
     * <p>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale (or even empty).
     *
     * @param keyspace the name of the keyspace to get replicas for.
     * @param partitionKey the partition key for which to find the set of
     * replica.
     * @return the (immutable) set of replicas for {@code partitionKey} as known
     * by the driver.
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
     * Returns the set of hosts that are replica for a given token range.
     * <p>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale (or even empty).
     *
     * @param keyspace the name of the keyspace to get replicas for.
     * @param range the token range.
     * @return the (immutable) set of replicas for {@code range} as known by the driver.
     */
    public Set<Host> getReplicas(String keyspace, TokenRange range) {
        keyspace = handleId(keyspace);
        TokenMap current = tokenMap;
        if (current == null) {
            return Collections.emptySet();
        } else {
            Set<Host> hosts = current.getReplicas(keyspace, range.getEnd());
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
     * Checks whether hosts that are currently up agree on the schema definition.
     * <p>
     * This method performs a one-time check only, without any form of retry; therefore {@link Cluster.Builder#withMaxSchemaAgreementWaitSeconds(int)}
     * does not apply in this case.
     *
     * @return {@code true} if all hosts agree on the schema; {@code false} if they don't agree, or if the check could not be performed
     * (for example, if the control connection is down).
     */
    public boolean checkSchemaAgreement() {
        try {
            return cluster.controlConnection.checkSchemaAgreement();
        } catch (Exception e) {
            logger.warn("Error while checking schema agreement", e);
            return false;
        }
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
     * Used when the keyspace name is unquoted and in the exact case we store it in
     * (typically when we got it from an internal call, not from the user).
     */
    KeyspaceMetadata getKeyspaceInternal(String keyspace) {
        return keyspaces.get(keyspace);
    }

    KeyspaceMetadata removeKeyspace(String keyspace) {
        KeyspaceMetadata removed = keyspaces.remove(keyspace);
        if (tokenMap != null)
            tokenMap.tokenToHosts.remove(keyspace);
        return removed;
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

    /**
     * Builds a new {@link Token} from its string representation, according to the partitioner
     * reported by the Cassandra nodes.
     *
     * @param tokenStr the string representation.
     * @return the token.
     */
    public Token newToken(String tokenStr) {
        TokenMap current = tokenMap;
        if (current == null)
            throw new DriverInternalError("Token factory not set. This should only happen at initialization time");

        return current.factory.fromString(tokenStr);
    }

    /**
     * Builds a new {@link TokenRange}.
     *
     * @param start the start token.
     * @param end the end token.
     * @return the range.
     */
    public TokenRange newTokenRange(Token start, Token end) {
        TokenMap current = tokenMap;
        if (current == null)
            throw new DriverInternalError("Token factory not set. This should only happen at initialization time");

        return new TokenRange(start, end, current.factory);
    }

    Token.Factory tokenFactory() {
        TokenMap current = tokenMap;
        return (current == null) ? null : current.factory;
    }

    static class TokenMap {

        private final Token.Factory factory;
        private final Map<String, Map<Token, Set<Host>>> tokenToHosts;
        private final Map<String, Map<Host, Set<TokenRange>>> hostsToRanges;
        private final List<Token> ring;
        private final Set<TokenRange> tokenRanges;
        final Set<Host> hosts;

        private TokenMap(Token.Factory factory,
                         Map<Host, Set<Token>> primaryToTokens,
                         Map<String, Map<Token, Set<Host>>> tokenToHosts,
                         Map<String, Map<Host, Set<TokenRange>>> hostsToRanges,
                         List<Token> ring, Set<TokenRange> tokenRanges, Set<Host> hosts) {
            this.factory = factory;
            this.tokenToHosts = tokenToHosts;
            this.hostsToRanges = hostsToRanges;
            this.ring = ring;
            this.tokenRanges = tokenRanges;
            this.hosts = hosts;
            for (Map.Entry<Host, Set<Token>> entry : primaryToTokens.entrySet()) {
                Host host = entry.getKey();
                host.setTokens(ImmutableSet.copyOf(entry.getValue()));
            }
        }

        public static TokenMap build(Token.Factory factory, Map<Host, Collection<String>> allTokens, Collection<KeyspaceMetadata> keyspaces) {

            Set<Host> hosts = allTokens.keySet();
            Map<Token, Host> tokenToPrimary = new HashMap<Token, Host>();
            Map<Host, Set<Token>> primaryToTokens = new HashMap<Host, Set<Token>>();
            Set<Token> allSorted = new TreeSet<Token>();

            for (Map.Entry<Host, Collection<String>> entry : allTokens.entrySet()) {
                Host host = entry.getKey();
                for (String tokenStr : entry.getValue()) {
                    try {
                        Token t = factory.fromString(tokenStr);
                        allSorted.add(t);
                        tokenToPrimary.put(t, host);
                        Set<Token> hostTokens = primaryToTokens.get(host);
                        if (hostTokens == null) {
                            hostTokens = new HashSet<Token>();
                            primaryToTokens.put(host, hostTokens);
                        }
                        hostTokens.add(t);
                    } catch (IllegalArgumentException e) {
                        // If we failed parsing that token, skip it
                    }
                }
            }

            List<Token> ring = new ArrayList<Token>(allSorted);
            Set<TokenRange> tokenRanges = makeTokenRanges(ring, factory);

            Map<String, Map<Token, Set<Host>>> tokenToHosts = new HashMap<String, Map<Token, Set<Host>>>();
            Map<String, Map<Host, Set<TokenRange>>> hostsToRanges = new HashMap<String, Map<Host, Set<TokenRange>>>();
            for (KeyspaceMetadata keyspace : keyspaces)
            {
                ReplicationStrategy strategy = keyspace.replicationStrategy();
                Map<Token, Set<Host>> ksTokens = (strategy == null)
                    ? makeNonReplicatedMap(tokenToPrimary)
                    : strategy.computeTokenToReplicaMap(tokenToPrimary, ring);

                tokenToHosts.put(keyspace.getName(), ksTokens);

                Map<Host, Set<TokenRange>> ksRanges;
                if (ring.size() == 1) {
                    // We forced the single range to ]minToken,minToken], make sure to use that instead of relying on the host's token
                    ImmutableMap.Builder<Host, Set<TokenRange>> builder = ImmutableMap.builder();
                    for (Host host : allTokens.keySet())
                        builder.put(host, tokenRanges);
                    ksRanges = builder.build();
                } else {
                    ksRanges = computeHostsToRangesMap(tokenRanges, ksTokens, hosts.size());
                }
                hostsToRanges.put(keyspace.getName(), ksRanges);
            }
            return new TokenMap(factory, primaryToTokens, tokenToHosts, hostsToRanges, ring, tokenRanges, hosts);
        }

        private Set<Host> getReplicas(String keyspace, Token token) {

            Map<Token, Set<Host>> keyspaceHosts = tokenToHosts.get(keyspace);
            if (keyspaceHosts == null)
                return Collections.emptySet();

            // If the token happens to be one of the "primary" tokens, get result directly
            Set<Host> hosts = keyspaceHosts.get(token);
            if (hosts != null)
                return hosts;

            // Otherwise, find closest "primary" token on the ring
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

        private static Set<TokenRange> makeTokenRanges(List<Token> ring, Token.Factory factory) {
            ImmutableSet.Builder<TokenRange> builder = ImmutableSet.builder();
            // JAVA-684: if there is only one token, return the range ]minToken, minToken]
            if(ring.size() == 1) {
                builder.add(new TokenRange(factory.minToken(), factory.minToken(), factory));                
            } else {
                for (int i = 0; i < ring.size(); i++) {
                    Token start = ring.get(i);
                    Token end = ring.get((i + 1) % ring.size());
                    builder.add(new TokenRange(start, end, factory));
                }
            }
            return builder.build();
        }

        private static Map<Host, Set<TokenRange>> computeHostsToRangesMap(Set<TokenRange> tokenRanges, Map<Token, Set<Host>> ksTokens, int hostCount) {
            Map<Host, ImmutableSet.Builder<TokenRange>> builders = Maps.newHashMapWithExpectedSize(hostCount);
            for (TokenRange range : tokenRanges) {
                Set<Host> replicas = ksTokens.get(range.getEnd());
                for (Host host : replicas) {
                    ImmutableSet.Builder<TokenRange> hostRanges = builders.get(host);
                    if (hostRanges == null) {
                        hostRanges = ImmutableSet.builder();
                        builders.put(host, hostRanges);
                    }
                    hostRanges.add(range);
                }
            }
            Map<Host, Set<TokenRange>> ksRanges = Maps.newHashMapWithExpectedSize(hostCount);
            for (Map.Entry<Host, ImmutableSet.Builder<TokenRange>> entry : builders.entrySet()) {
                ksRanges.put(entry.getKey(), entry.getValue().build());
            }
            return ksRanges;
        }
    }
}
