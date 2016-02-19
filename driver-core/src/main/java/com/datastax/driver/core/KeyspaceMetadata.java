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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Describes a keyspace defined in this cluster.
 */
public class KeyspaceMetadata {

    public static final String KS_NAME = "keyspace_name";
    private static final String DURABLE_WRITES = "durable_writes";
    private static final String STRATEGY_CLASS = "strategy_class";
    private static final String STRATEGY_OPTIONS = "strategy_options";
    private static final String REPLICATION = "replication";

    private final String name;
    private final boolean durableWrites;

    private final ReplicationStrategy strategy;
    private final Map<String, String> replication;

    final Map<String, TableMetadata> tables = new ConcurrentHashMap<String, TableMetadata>();
    final Map<String, MaterializedViewMetadata> views = new ConcurrentHashMap<String, MaterializedViewMetadata>();
    final Map<String, UserType> userTypes = new ConcurrentHashMap<String, UserType>();
    final Map<String, FunctionMetadata> functions = new ConcurrentHashMap<String, FunctionMetadata>();
    final Map<String, AggregateMetadata> aggregates = new ConcurrentHashMap<String, AggregateMetadata>();

    @VisibleForTesting
    KeyspaceMetadata(String name, boolean durableWrites, Map<String, String> replication) {
        this.name = name;
        this.durableWrites = durableWrites;
        this.replication = replication;
        this.strategy = ReplicationStrategy.create(replication);
    }

    static KeyspaceMetadata build(Row row, VersionNumber cassandraVersion) {
        if (cassandraVersion.getMajor() <= 2) {
            String name = row.getString(KS_NAME);
            boolean durableWrites = row.getBool(DURABLE_WRITES);
            Map<String, String> replicationOptions;
            replicationOptions = new HashMap<String, String>();
            replicationOptions.put("class", row.getString(STRATEGY_CLASS));
            replicationOptions.putAll(SimpleJSONParser.parseStringMap(row.getString(STRATEGY_OPTIONS)));
            return new KeyspaceMetadata(name, durableWrites, replicationOptions);
        } else {
            String name = row.getString(KS_NAME);
            boolean durableWrites = row.getBool(DURABLE_WRITES);
            return new KeyspaceMetadata(name, durableWrites, row.getMap(REPLICATION, String.class, String.class));
        }
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

    TableMetadata removeTable(String table) {
        return tables.remove(table);
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
     * Returns the metadata for a materialized view contained in this keyspace.
     *
     * @param name the name of materialized view to retrieve
     * @return the metadata for materialized view {@code name} if it exists in this keyspace,
     * {@code null} otherwise.
     */
    public MaterializedViewMetadata getMaterializedView(String name) {
        return views.get(Metadata.handleId(name));
    }

    MaterializedViewMetadata removeMaterializedView(String materializedView) {
        return views.remove(materializedView);
    }

    /**
     * Returns the materialized views defined in this keyspace.
     *
     * @return a collection of the metadata for the materialized views defined in this
     * keyspace.
     */
    public Collection<MaterializedViewMetadata> getMaterializedViews() {
        return Collections.unmodifiableCollection(views.values());
    }

    /**
     * Returns the definition for a user defined type (UDT) in this keyspace.
     *
     * @param name the name of UDT definition to retrieve
     * @return the definition for {@code name} if it exists in this keyspace,
     * {@code null} otherwise.
     */
    public UserType getUserType(String name) {
        return userTypes.get(Metadata.handleId(name));
    }

    /**
     * Returns the user types defined in this keyspace.
     *
     * @return a collection of the definition for the user types defined in this
     * keyspace.
     */
    public Collection<UserType> getUserTypes() {
        return Collections.unmodifiableCollection(userTypes.values());
    }

    UserType removeUserType(String userType) {
        return userTypes.remove(userType);
    }

    /**
     * Returns the definition of a function in this keyspace.
     *
     * @param name          the name of the function.
     * @param argumentTypes the types of the function's arguments.
     * @return the function definition if it exists in this keyspace, {@code null} otherwise.
     */
    public FunctionMetadata getFunction(String name, Collection<DataType> argumentTypes) {
        return functions.get(Metadata.fullFunctionName(Metadata.handleId(name), argumentTypes));
    }

    /**
     * Returns the definition of a function in this keyspace.
     *
     * @param name          the name of the function.
     * @param argumentTypes the types of the function's arguments.
     * @return the function definition if it exists in this keyspace, {@code null} otherwise.
     */
    public FunctionMetadata getFunction(String name, DataType... argumentTypes) {
        return getFunction(name, Lists.newArrayList(argumentTypes));
    }

    /**
     * Returns the functions defined in this keyspace.
     *
     * @return a collection of the definition for the functions defined in this
     * keyspace.
     */
    public Collection<FunctionMetadata> getFunctions() {
        return Collections.unmodifiableCollection(functions.values());
    }

    FunctionMetadata removeFunction(String fullName) {
        return functions.remove(fullName);
    }

    /**
     * Returns the definition of an aggregate in this keyspace.
     *
     * @param name          the name of the aggregate.
     * @param argumentTypes the types of the aggregate's arguments.
     * @return the aggregate definition if it exists in this keyspace, {@code null} otherwise.
     */
    public AggregateMetadata getAggregate(String name, Collection<DataType> argumentTypes) {
        return aggregates.get(Metadata.fullFunctionName(Metadata.handleId(name), argumentTypes));
    }

    /**
     * Returns the definition of an aggregate in this keyspace.
     *
     * @param name          the name of the aggregate.
     * @param argumentTypes the types of the aggregate's arguments.
     * @return the aggregate definition if it exists in this keyspace, {@code null} otherwise.
     */
    public AggregateMetadata getAggregate(String name, DataType... argumentTypes) {
        return getAggregate(name, Lists.newArrayList(argumentTypes));
    }

    /**
     * Returns the aggregates defined in this keyspace.
     *
     * @return a collection of the definition for the aggregates defined in this
     * keyspace.
     */
    public Collection<AggregateMetadata> getAggregates() {
        return Collections.unmodifiableCollection(aggregates.values());
    }

    AggregateMetadata removeAggregate(String fullName) {
        return aggregates.remove(fullName);
    }

    /**
     * Returns a {@code String} containing CQL queries representing this
     * keyspace and the user types and tables it contains.
     * <p/>
     * In other words, this method returns the queries that would allow to
     * recreate the schema of this keyspace, along with all its user
     * types/tables.
     * <p/>
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     *
     * @return the CQL queries representing this keyspace schema as a {code
     * String}.
     */
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(asCQLQuery()).append('\n');

        for (UserType udt : userTypes.values())
            sb.append('\n').append(udt.exportAsString()).append('\n');

        for (TableMetadata tm : tables.values())
            sb.append('\n').append(tm.exportAsString()).append('\n');

        for (FunctionMetadata fm : functions.values())
            sb.append('\n').append(fm.exportAsString()).append('\n');

        for (AggregateMetadata am : aggregates.values())
            sb.append('\n').append(am.exportAsString()).append('\n');

        return sb.toString();
    }

    /**
     * Returns a CQL query representing this keyspace.
     * <p/>
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        KeyspaceMetadata that = (KeyspaceMetadata) o;

        if (durableWrites != that.durableWrites)
            return false;
        if (!name.equals(that.name))
            return false;
        if (strategy != null ? !strategy.equals(that.strategy) : that.strategy != null)
            return false;
        if (!replication.equals(that.replication))
            return false;
        return tables.equals(that.tables);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (durableWrites ? 1 : 0);
        result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
        result = 31 * result + replication.hashCode();
        result = 31 * result + tables.hashCode();
        return result;
    }

    void add(TableMetadata tm) {
        tables.put(tm.getName(), tm);
    }

    void add(MaterializedViewMetadata view) {
        views.put(view.getName(), view);
    }

    void add(FunctionMetadata function) {
        String functionName = Metadata.fullFunctionName(function.getSimpleName(), function.getArguments().values());
        functions.put(functionName, function);
    }

    void add(AggregateMetadata aggregate) {
        String aggregateName = Metadata.fullFunctionName(aggregate.getSimpleName(), aggregate.getArgumentTypes());
        aggregates.put(aggregateName, aggregate);
    }

    void add(UserType type) {
        userTypes.put(type.getTypeName(), type);
    }

    ReplicationStrategy replicationStrategy() {
        return strategy;
    }

}
