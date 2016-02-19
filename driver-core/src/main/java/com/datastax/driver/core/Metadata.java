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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 */
public class Metadata {

    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);

    final Cluster.Manager cluster;
    volatile String clusterName;
    volatile String partitioner;
    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();
    final ConcurrentMap<String, KeyspaceMetadata> keyspaces = new ConcurrentHashMap<String, KeyspaceMetadata>();
    volatile TokenMap tokenMap;

    final ReentrantLock lock = new ReentrantLock();

    private static final Pattern alphanumeric = Pattern.compile("\\w+"); // this includes _
    private static final Pattern lowercaseAlphanumeric = Pattern.compile("[a-z][a-z0-9_]*");

    Metadata(Cluster.Manager cluster) {
        this.cluster = cluster;
    }

    void rebuildTokenMap(String partitioner, Map<Host, Collection<String>> allTokens) {
        lock.lock();
        try {
            if (allTokens.isEmpty())
                return;

            Token.Factory factory = partitioner == null
                    ? (tokenMap == null ? null : tokenMap.factory)
                    : Token.getFactory(partitioner);
            if (factory == null)
                return;

            this.tokenMap = TokenMap.build(factory, allTokens, keyspaces.values());
        } finally {
            lock.unlock();
        }
    }

    Host newHost(InetSocketAddress address) {
        return new Host(address, cluster.convictionPolicyFactory, cluster);
    }

    Host addIfAbsent(Host host) {
        Host previous = hosts.putIfAbsent(host.getSocketAddress(), host);
        return previous == null ? host : null;
    }

    Host add(InetSocketAddress address) {
        return addIfAbsent(newHost(address));
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

    /*
     * Deal with case sensitivity for a given element id (keyspace, table, column, etc.)
     *
     * This method is used to convert identifiers provided by the client (through methods such as getKeyspace(String)),
     * to the format used internally by the driver.
     *
     * We expect client-facing APIs to behave like cqlsh, that is:
     * - identifiers that are mixed-case or contain special characters should be quoted.
     * - unquoted identifiers will be lowercased: getKeyspace("Foo") will look for a keyspace named "foo"
     */
    static String handleId(String id) {
        // Shouldn't really happen for this method, but no reason to fail here
        if (id == null)
            return null;

        if (alphanumeric.matcher(id).matches())
            return id.toLowerCase();

        // Check if it's enclosed in quotes. If it is, remove them and unescape internal double quotes
        if (!id.isEmpty() && id.charAt(0) == '"' && id.charAt(id.length() - 1) == '"')
            return id.substring(1, id.length() - 1).replaceAll("\"\"", "\"");

        // Otherwise, just return the id.
        // Note that this is a bit at odds with the rules explained above, because the client can pass an
        // identifier that contains special characters, without the need to quote it.
        // Still it's better to be lenient here rather than throwing an exception.
        return id;
    }

    // Escape a CQL3 identifier based on its value as read from the schema
    // tables. Because it comes from Cassandra, we could just always quote it,
    // but to get a nicer output we don't do it if it's not necessary.
    static String escapeId(String ident) {
        // we don't need to escape if it's lowercase and match non-quoted CQL3 ids.
        return lowercaseAlphanumeric.matcher(ident).matches() ? ident : quote(ident);
    }

    /**
     * Builds the internal name of a function/aggregate, which is similar, but not identical,
     * to the function/aggregate signature.
     * This is only used to generate keys for internal metadata maps (KeyspaceMetadata.functions and.
     * KeyspaceMetadata.aggregates).
     * Note that if simpleName comes from the user, the caller must call handleId on it before passing it to this method.
     * Note that this method does not necessarily generates a valid CQL function signature.
     * Note that argumentTypes can be either a list of strings (schema change events)
     * or a list of DataTypes (function lookup from client code).
     * This method must ensure that both cases produce the same identifier.
     */
    static String fullFunctionName(String simpleName, Collection<?> argumentTypes) {
        StringBuilder sb = new StringBuilder(simpleName);
        sb.append('(');
        boolean first = true;
        for (Object argumentType : argumentTypes) {
            if (first)
                first = false;
            else
                sb.append(',');
            // user types must be represented by their names only,
            // without keyspace prefix, because that's how
            // they appear in a schema change event (in targetSignature)
            if (argumentType instanceof UserType) {
                UserType userType = (UserType) argumentType;
                String typeName = Metadata.escapeId(userType.getTypeName());
                if (userType.isFrozen())
                    sb.append("frozen<");
                sb.append(typeName);
                if (userType.isFrozen())
                    sb.append(">");
            } else {
                sb.append(argumentType);
            }
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * Quote a keyspace, table or column identifier to make it case sensitive.
     * <p/>
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
        return '"' + id.replace("\"", "\"\"") + '"';
    }

    /**
     * Returns the token ranges that define data distribution in the ring.
     * <p/>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale.
     *
     * @return the token ranges. Note that the result might be stale or empty if
     * metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
     */
    public Set<TokenRange> getTokenRanges() {
        TokenMap current = tokenMap;
        return (current == null) ? Collections.<TokenRange>emptySet() : current.tokenRanges;
    }

    /**
     * Returns the token ranges that are replicated on the given host, for the given
     * keyspace.
     * <p/>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale (or even empty).
     *
     * @param keyspace the name of the keyspace to get token ranges for.
     * @param host     the host.
     * @return the (immutable) set of token ranges for {@code host} as known
     * by the driver. Note that the result might be stale or empty if metadata
     * was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
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
     * <p/>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale (or even empty).
     *
     * @param keyspace     the name of the keyspace to get replicas for.
     * @param partitionKey the partition key for which to find the set of
     *                     replica.
     * @return the (immutable) set of replicas for {@code partitionKey} as known
     * by the driver. Note that the result might be stale or empty if metadata was
     * explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
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
     * <p/>
     * Note that this information is refreshed asynchronously by the control
     * connection, when schema or ring topology changes. It might occasionally
     * be stale (or even empty).
     *
     * @param keyspace the name of the keyspace to get replicas for.
     * @param range    the token range.
     * @return the (immutable) set of replicas for {@code range} as known by the driver.
     * Note that the result might be stale or empty if metadata was explicitly disabled
     * with {@link QueryOptions#setMetadataEnabled(boolean)}.
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
     * <p/>
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
     *                 returned.
     * @return the metadata of the requested keyspace or {@code null} if {@code
     * keyspace} is not a known keyspace. Note that the result might be stale or null if
     * metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
     */
    public KeyspaceMetadata getKeyspace(String keyspace) {
        return keyspaces.get(handleId(keyspace));
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
     * @return a list of all the defined keyspaces. Note that the result might be stale or empty if
     * metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
     */
    public List<KeyspaceMetadata> getKeyspaces() {
        return new ArrayList<KeyspaceMetadata>(keyspaces.values());
    }

    /**
     * Returns a {@code String} containing CQL queries representing the schema
     * of this cluster.
     * <p/>
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this cluster.
     * <p/>
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     * <p/>
     * It might be stale or empty if metadata was explicitly disabled with
     * {@link QueryOptions#setMetadataEnabled(boolean)}.
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
     * Creates a tuple type given a list of types.
     *
     * @param types the types for the tuple type.
     * @return the newly created tuple type.
     */
    public TupleType newTupleType(DataType... types) {
        return newTupleType(Arrays.asList(types));
    }

    /**
     * Creates a tuple type given a list of types.
     *
     * @param types the types for the tuple type.
     * @return the newly created tuple type.
     */
    public TupleType newTupleType(List<DataType> types) {
        return new TupleType(types, cluster.protocolVersion(), cluster.configuration.getCodecRegistry());
    }

    /**
     * Builds a new {@link Token} from its string representation, according to the partitioner
     * reported by the Cassandra nodes.
     *
     * @param tokenStr the string representation.
     * @return the token.
     * @throws IllegalStateException if the token factory was not initialized. This would typically
     *                               happen if metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}
     *                               before startup.
     */
    public Token newToken(String tokenStr) {
        TokenMap current = tokenMap;
        if (current == null)
            throw new IllegalStateException("Token factory not set. This should only happen if metadata was explicitly disabled");
        return current.factory.fromString(tokenStr);
    }

    /**
     * Builds a new {@link TokenRange}.
     *
     * @param start the start token.
     * @param end   the end token.
     * @return the range.
     * @throws IllegalStateException if the token factory was not initialized. This would typically
     *                               happen if metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}
     *                               before startup.
     */
    public TokenRange newTokenRange(Token start, Token end) {
        TokenMap current = tokenMap;
        if (current == null)
            throw new IllegalStateException("Token factory not set. This should only happen if metadata was explicitly disabled");

        return new TokenRange(start, end, current.factory);
    }

    Token.Factory tokenFactory() {
        TokenMap current = tokenMap;
        return (current == null) ? null : current.factory;
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

    void triggerOnUserTypeAdded(UserType type) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onUserTypeAdded(type);
        }
    }

    void triggerOnUserTypeChanged(UserType current, UserType previous) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onUserTypeChanged(current, previous);
        }
    }

    void triggerOnUserTypeRemoved(UserType type) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onUserTypeRemoved(type);
        }
    }

    void triggerOnFunctionAdded(FunctionMetadata function) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onFunctionAdded(function);
        }
    }

    void triggerOnFunctionChanged(FunctionMetadata current, FunctionMetadata previous) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onFunctionChanged(current, previous);
        }
    }

    void triggerOnFunctionRemoved(FunctionMetadata function) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onFunctionRemoved(function);
        }
    }

    void triggerOnAggregateAdded(AggregateMetadata aggregate) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onAggregateAdded(aggregate);
        }
    }

    void triggerOnAggregateChanged(AggregateMetadata current, AggregateMetadata previous) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onAggregateChanged(current, previous);
        }
    }

    void triggerOnAggregateRemoved(AggregateMetadata aggregate) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onAggregateRemoved(aggregate);
        }
    }

    void triggerOnMaterializedViewAdded(MaterializedViewMetadata view) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onMaterializedViewAdded(view);
        }
    }

    void triggerOnMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onMaterializedViewChanged(current, previous);
        }
    }

    void triggerOnMaterializedViewRemoved(MaterializedViewMetadata view) {
        for (SchemaChangeListener listener : cluster.schemaChangeListeners) {
            listener.onMaterializedViewRemoved(view);
        }
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
            Map<ReplicationStrategy, Map<Token, Set<Host>>> replStrategyToHosts = new HashMap<ReplicationStrategy, Map<Token, Set<Host>>>();
            Map<String, Map<Host, Set<TokenRange>>> hostsToRanges = new HashMap<String, Map<Host, Set<TokenRange>>>();
            for (KeyspaceMetadata keyspace : keyspaces) {
                ReplicationStrategy strategy = keyspace.replicationStrategy();
                Map<Token, Set<Host>> ksTokens = replStrategyToHosts.get(strategy);
                if (ksTokens == null) {
                    ksTokens = (strategy == null)
                            ? makeNonReplicatedMap(tokenToPrimary)
                            : strategy.computeTokenToReplicaMap(keyspace.getName(), tokenToPrimary, ring);
                    replStrategyToHosts.put(strategy, ksTokens);
                }

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
            if (ring.size() == 1) {
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
