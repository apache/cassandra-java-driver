/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    private volatile TokenMap tokenMap;

    final ReentrantLock lock = new ReentrantLock();

    // See https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#appendixA
    private static final Set<String> RESERVED_KEYWORDS = ImmutableSet.of(
            "add", "allow", "alter", "and", "any", "apply", "asc", "authorize", "batch", "begin", "by",
            "columnfamily", "create", "delete", "desc", "drop", "each_quorum", "from", "grant", "in",
            "index", "inet", "infinity", "insert", "into", "keyspace", "keyspaces", "limit", "local_one",
            "local_quorum", "modify", "nan", "norecursive", "of", "on", "one", "order", "password",
            "primary", "quorum", "rename", "revoke", "schema", "select", "set", "table", "to",
            "token", "three", "truncate", "two", "unlogged", "update", "use", "using", "where", "with"
    );

    Metadata(Cluster.Manager cluster) {
        this.cluster = cluster;
    }

    // rebuilds the token map with the current hosts, typically when refreshing schema metadata
    void rebuildTokenMap() {
        lock.lock();
        try {
            if (tokenMap == null)
                return;
            this.tokenMap = TokenMap.build(
                    tokenMap.factory,
                    tokenMap.primaryToTokens,
                    keyspaces.values(),
                    tokenMap.ring,
                    tokenMap.tokenRanges,
                    tokenMap.tokenToPrimary);
        } finally {
            lock.unlock();
        }
    }

    // rebuilds the token map for a new set of hosts, typically when refreshing nodes list
    void rebuildTokenMap(Token.Factory factory, Map<Host, Set<Token>> allTokens) {
        lock.lock();
        try {
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

        if (isAlphanumeric(id))
            return id.toLowerCase();

        // Check if it's enclosed in quotes. If it is, remove them and unescape internal double quotes
        return ParseUtils.unDoubleQuote(id);
    }

    private static boolean isAlphanumeric(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!(
                    (c >= 48 && c <= 57) // 0-9
                            || (c >= 65 && c <= 90) // A-Z
                            || (c == 95) // _ (underscore)
                            || (c >= 97 && c <= 122) // a-z
            ))
                return false;
        }
        return true;
    }

    /**
     * Quotes a CQL identifier if necessary.
     * <p/>
     * This is similar to {@link #quote(String)}, except that it won't quote the input string
     * if it can safely be used as-is. For example:
     * <ul>
     * <li>{@code quoteIfNecessary("foo").equals("foo")} (no need to quote).</li>
     * <li>{@code quoteIfNecessary("Foo").equals("\"Foo\"")} (identifier is mixed case so case
     * sensitivity is required)</li>
     * <li>{@code quoteIfNecessary("foo bar").equals("\"foo bar\"")} (identifier contains
     * special characters)</li>
     * <li>{@code quoteIfNecessary("table").equals("\"table\"")} (identifier is a reserved CQL
     * keyword)</li>
     * </ul>
     *
     * @param id the "internal" form of the identifier. That is, the identifier as it would
     *           appear in Cassandra system tables (such as {@code system_schema.tables},
     *           {@code system_schema.columns}, etc.)
     * @return the identifier as it would appear in a CQL query string. This is also how you need
     * to pass it to public driver methods, such as {@link #getKeyspace(String)}.
     */
    public static String quoteIfNecessary(String id) {
        return needsQuote(id)
                ? quote(id)
                : id;
    }

    /**
     * We don't need to escape an identifier if it
     * matches non-quoted CQL3 ids ([a-z][a-z0-9_]*),
     * and if it's not a CQL reserved keyword.
     */
    private static boolean needsQuote(String s) {
        // this method should only be called for C*-provided identifiers,
        // so we expect it to be non-null and non-empty.
        assert s != null && !s.isEmpty();
        char c = s.charAt(0);
        if (!(c >= 97 && c <= 122)) // a-z
            return true;
        for (int i = 1; i < s.length(); i++) {
            c = s.charAt(i);
            if (!(
                    (c >= 48 && c <= 57) // 0-9
                            || (c == 95) // _
                            || (c >= 97 && c <= 122) // a-z
            )) {
                return true;
            }
        }
        return isReservedCqlKeyword(s);
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
                String typeName = Metadata.quoteIfNecessary(userType.getTypeName());
                sb.append(typeName);
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
     * can be used to enclose such identifiers in double quotes, making them case
     * sensitive.
     * <p/>
     * Note that
     * <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/keywords_r.html">reserved CQL keywords</a>
     * should also be quoted. You can check if a given identifier is a reserved keyword
     * by calling {@link #isReservedCqlKeyword(String)}.
     *
     * @param id the keyspace or table identifier.
     * @return {@code id} enclosed in double-quotes, for use in methods like
     * {@link #getReplicas}, {@link #getKeyspace}, {@link KeyspaceMetadata#getTable}
     * or even {@link Cluster#connect(String)}.
     */
    public static String quote(String id) {
        return ParseUtils.doubleQuote(id);
    }

    /**
     * Checks whether an identifier is a known reserved CQL keyword or not.
     * <p/>
     * The check is case-insensitive, i.e., the word "{@code KeYsPaCe}"
     * would be considered as a reserved CQL keyword just as "{@code keyspace}".
     * <p/>
     * Note: The list of reserved CQL keywords is subject to change in future
     * versions of Cassandra. As a consequence, this method is provided solely as a
     * convenience utility and should not be considered as an authoritative
     * source of truth for checking reserved CQL keywords.
     *
     * @param id the identifier to check; should not be {@code null}.
     * @return {@code true} if the given identifier is a known reserved
     * CQL keyword, {@code false} otherwise.
     */
    public static boolean isReservedCqlKeyword(String id) {
        return id != null && RESERVED_KEYWORDS.contains(id.toLowerCase());
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
            Map<Host, Set<TokenRange>> dcRanges = current.hostsToRangesByKeyspace.get(keyspace);
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
     * Note that it is assumed that the input range does not overlap across multiple host ranges.
     * If the range extends over multiple hosts, it only returns the replicas for those hosts
     * that are replicas for the last token of the range.  This behavior may change in a future
     * release, see <a href="https://datastax-oss.atlassian.net/browse/JAVA-1355">JAVA-1355</a>.
     * <p/>
     * Also note that this information is refreshed asynchronously by the control
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
            tokenMap.tokenToHostsByKeyspace.remove(keyspace);
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
     * Builds a new {@link Token} from a partition key.
     *
     * @param components the components of the partition key, in their serialized form (obtained with
     *                   {@link TypeCodec#serialize(Object, ProtocolVersion)}).
     * @return the token.
     * @throws IllegalStateException if the token factory was not initialized. This would typically
     *                               happen if metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}
     *                               before startup.
     */
    public Token newToken(ByteBuffer... components) {
        TokenMap current = tokenMap;
        if (current == null)
            throw new IllegalStateException("Token factory not set. This should only happen if metadata was explicitly disabled");
        return current.factory.hash(
                SimpleStatement.compose(components));
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

    private static class TokenMap {

        private final Token.Factory factory;
        private final Map<Host, Set<Token>> primaryToTokens;
        private final Map<String, Map<Token, Set<Host>>> tokenToHostsByKeyspace;
        private final Map<String, Map<Host, Set<TokenRange>>> hostsToRangesByKeyspace;
        private final List<Token> ring;
        private final Set<TokenRange> tokenRanges;
        private final Map<Token, Host> tokenToPrimary;

        private TokenMap(Token.Factory factory,
                         List<Token> ring,
                         Set<TokenRange> tokenRanges,
                         Map<Token, Host> tokenToPrimary,
                         Map<Host, Set<Token>> primaryToTokens,
                         Map<String, Map<Token, Set<Host>>> tokenToHostsByKeyspace,
                         Map<String, Map<Host, Set<TokenRange>>> hostsToRangesByKeyspace) {
            this.factory = factory;
            this.ring = ring;
            this.tokenRanges = tokenRanges;
            this.tokenToPrimary = tokenToPrimary;
            this.primaryToTokens = primaryToTokens;
            this.tokenToHostsByKeyspace = tokenToHostsByKeyspace;
            this.hostsToRangesByKeyspace = hostsToRangesByKeyspace;
            for (Map.Entry<Host, Set<Token>> entry : primaryToTokens.entrySet()) {
                Host host = entry.getKey();
                host.setTokens(ImmutableSet.copyOf(entry.getValue()));
            }
        }

        private static TokenMap build(Token.Factory factory, Map<Host, Set<Token>> allTokens, Collection<KeyspaceMetadata> keyspaces) {
            Map<Token, Host> tokenToPrimary = new HashMap<Token, Host>();
            Set<Token> allSorted = new TreeSet<Token>();
            for (Map.Entry<Host, ? extends Collection<Token>> entry : allTokens.entrySet()) {
                Host host = entry.getKey();
                for (Token t : entry.getValue()) {
                    try {
                        allSorted.add(t);
                        tokenToPrimary.put(t, host);
                    } catch (IllegalArgumentException e) {
                        // If we failed parsing that token, skip it
                    }
                }
            }
            List<Token> ring = new ArrayList<Token>(allSorted);
            Set<TokenRange> tokenRanges = makeTokenRanges(ring, factory);
            return build(factory, allTokens, keyspaces, ring, tokenRanges, tokenToPrimary);
        }

        private static TokenMap build(Token.Factory factory, Map<Host, Set<Token>> allTokens, Collection<KeyspaceMetadata> keyspaces, List<Token> ring, Set<TokenRange> tokenRanges, Map<Token, Host> tokenToPrimary) {
            Set<Host> hosts = allTokens.keySet();
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
            return new TokenMap(factory, ring, tokenRanges, tokenToPrimary, allTokens, tokenToHosts, hostsToRanges);
        }

        private Set<Host> getReplicas(String keyspace, Token token) {

            Map<Token, Set<Host>> tokenToHosts = tokenToHostsByKeyspace.get(keyspace);
            if (tokenToHosts == null)
                return Collections.emptySet();

            // If the token happens to be one of the "primary" tokens, get result directly
            Set<Host> hosts = tokenToHosts.get(token);
            if (hosts != null)
                return hosts;

            // Otherwise, find closest "primary" token on the ring
            int i = Collections.binarySearch(ring, token);
            if (i < 0) {
                i = -i - 1;
                if (i >= ring.size())
                    i = 0;
            }

            return tokenToHosts.get(ring.get(i));
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
