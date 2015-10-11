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

import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.SchemaElement.*;

abstract class SchemaParser {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaParser.class);

    private static final TypeCodec.ListCodec<String> LIST_OF_TEXT_CODEC = new TypeCodec.ListCodec<String>(TypeCodec.VarcharCodec.instance);

    private static final SchemaParser V2_PARSER = new V2SchemaParser();
    private static final SchemaParser V3_PARSER = new V3SchemaParser();

    static SchemaParser forVersion(VersionNumber cassandraVersion) {
        if(cassandraVersion.getMajor() >= 3) return V3_PARSER;
        return V2_PARSER;
    }

    abstract SystemRows fetchSystemRows(Metadata metadata,
                                        SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                                        Connection connection, VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException;

    abstract String tableNameColumn();

    void refresh(Metadata metadata,
                 SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                 Connection connection, VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

        SystemRows rows = fetchSystemRows(metadata, targetType, targetKeyspace, targetName, targetSignature, connection, cassandraVersion);

        metadata.lock.lock();
        try {
            if (targetType == null || targetType == KEYSPACE) {
                // building the whole schema or a keyspace
                assert rows.keyspaces != null;
                Map<String, KeyspaceMetadata> keyspaces = buildKeyspaces(metadata, rows, cassandraVersion);
                updateKeyspaces(metadata, metadata.keyspaces, keyspaces, targetKeyspace);
            } else {
                assert targetKeyspace != null;
                KeyspaceMetadata keyspace = metadata.keyspaces.get(targetKeyspace);
                // If we update a keyspace we don't know about, something went
                // wrong. Log an error and schedule a full schema rebuild.
                if (keyspace == null) {
                    logger.info(String.format("Asked to rebuild %s %s.%s but I don't know keyspace %s",
                        targetType, targetKeyspace, targetName, targetKeyspace));
                    metadata.cluster.submitSchemaRefresh(null, null, null, null);
                } else {
                    switch (targetType) {
                        case TABLE:
                            if (rows.tables.containsKey(targetKeyspace)) {
                                Map<String, TableMetadata> tables = buildTables(metadata, keyspace, rows.tables.get(targetKeyspace), rows.columns.get(targetKeyspace), rows.indexes.get(targetKeyspace), cassandraVersion);
                                updateTables(metadata, keyspace.tables, tables, targetName);
                            }
                            if (rows.views.containsKey(targetKeyspace)) {
                                Map<String, MaterializedViewMetadata> tables = buildViews(keyspace, rows.views.get(targetKeyspace), rows.columns.get(targetKeyspace), cassandraVersion);
                                updateViews(metadata, keyspace.views, tables, targetName);
                            }
                            break;
                        case TYPE:
                            if (rows.udts.containsKey(targetKeyspace)) {
                                Map<String, UserType> userTypes = buildUserTypes(metadata, rows.udts.get(targetKeyspace));
                                updateUserTypes(metadata, keyspace.userTypes, userTypes, targetName);
                            }
                            break;
                        case FUNCTION:
                            if (rows.functions.containsKey(targetKeyspace)) {
                                Map<String, FunctionMetadata> functions = buildFunctions(metadata, keyspace, rows.functions.get(targetKeyspace));
                                updateFunctions(metadata, keyspace.functions, functions, targetName);
                            }
                            break;
                        case AGGREGATE:
                            if (rows.aggregates.containsKey(targetKeyspace)) {
                                Map<String, AggregateMetadata> aggregates = buildAggregates(metadata, keyspace, rows.aggregates.get(targetKeyspace));
                                updateAggregates(metadata, keyspace.aggregates, aggregates, targetName);
                            }
                            break;
                    }
                }
            }
        } finally {
            metadata.lock.unlock();
        }
    }

    private Map<String, KeyspaceMetadata> buildKeyspaces(Metadata metadata,
                                                         SystemRows rows,
                                                         VersionNumber cassandraVersion) {

        Map<String, KeyspaceMetadata> keyspaces = new LinkedHashMap<String, KeyspaceMetadata>();
        for (Row keyspaceRow : rows.keyspaces) {
            KeyspaceMetadata keyspace = KeyspaceMetadata.build(keyspaceRow, cassandraVersion);
            Map<String, TableMetadata> tables = buildTables(metadata, keyspace, rows.tables.get(keyspace.getName()), rows.columns.get(keyspace.getName()), rows.indexes.get(keyspace.getName()), cassandraVersion);
            for (TableMetadata table : tables.values()) {
                keyspace.add(table);
            }
            Map<String, UserType> userTypes = buildUserTypes(metadata, rows.udts.get(keyspace.getName()));
            for (UserType userType : userTypes.values()) {
                keyspace.add(userType);
            }
            Map<String, FunctionMetadata> functions = buildFunctions(metadata, keyspace, rows.functions.get(keyspace.getName()));
            for (FunctionMetadata function : functions.values()) {
                keyspace.add(function);
            }
            Map<String, AggregateMetadata> aggregates = buildAggregates(metadata, keyspace, rows.aggregates.get(keyspace.getName()));
            for (AggregateMetadata aggregate : aggregates.values()) {
                keyspace.add(aggregate);
            }
            Map<String, MaterializedViewMetadata> views = buildViews(keyspace, rows.views.get(keyspace.getName()), rows.columns.get(keyspace.getName()), cassandraVersion);
            for (MaterializedViewMetadata view : views.values()) {
                keyspace.add(view);
            }
            keyspaces.put(keyspace.getName(), keyspace);
        }
        return keyspaces;
    }

    private Map<String, TableMetadata> buildTables(Metadata metadata, KeyspaceMetadata keyspace, List<Row> tableRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, Map<String, List<Row>> indexDefs, VersionNumber cassandraVersion) {
        Map<String, TableMetadata> tables = new LinkedHashMap<String, TableMetadata>();
        if (tableRows != null) {
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();
            CodecRegistry codecRegistry = metadata.cluster.configuration.getCodecRegistry();

            for (Row tableDef : tableRows) {
                String cfName = tableDef.getString(tableNameColumn());
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
                    List<Row> cfIndexes = (indexDefs == null) ? null : indexDefs.get(cfName);
                    TableMetadata table = TableMetadata.build(keyspace, tableDef, cols, cfIndexes, tableNameColumn(), cassandraVersion, protocolVersion, codecRegistry);
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

    private Map<String, UserType> buildUserTypes(Metadata metadata, List<Row> udtRows) {
        Map<String, UserType> userTypes = new LinkedHashMap<String, UserType>();
        if (udtRows != null) {
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();
            CodecRegistry codecRegistry = metadata.cluster.configuration.getCodecRegistry();
            for (Row udtRow : udtRows) {
                UserType type = UserType.build(udtRow, protocolVersion, codecRegistry);
                userTypes.put(type.getTypeName(), type);
            }
        }
        return userTypes;
    }

    private Map<String, FunctionMetadata> buildFunctions(Metadata metadata, KeyspaceMetadata keyspace, List<Row> functionRows) {
        Map<String, FunctionMetadata> functions = new LinkedHashMap<String, FunctionMetadata>();
        if (functionRows != null) {
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();
            CodecRegistry codecRegistry = metadata.cluster.configuration.getCodecRegistry();
            for (Row functionRow : functionRows) {
                FunctionMetadata function = FunctionMetadata.build(keyspace, functionRow, protocolVersion, codecRegistry);
                if (function != null)
                    functions.put(function.getFullName(), function);
            }
        }
        return functions;
    }

    private Map<String, AggregateMetadata> buildAggregates(Metadata metadata, KeyspaceMetadata keyspace, List<Row> aggregateRows) {
        Map<String, AggregateMetadata> aggregates = new LinkedHashMap<String, AggregateMetadata>();
        if (aggregateRows != null) {
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();
            CodecRegistry codecRegistry = metadata.cluster.configuration.getCodecRegistry();
            for (Row aggregateRow : aggregateRows) {
                AggregateMetadata aggregate = AggregateMetadata.build(keyspace, aggregateRow, protocolVersion, codecRegistry);
                if (aggregate != null)
                    aggregates.put(aggregate.getFullName(), aggregate);
            }
        }
        return aggregates;
    }

    private Map<String, MaterializedViewMetadata> buildViews(KeyspaceMetadata keyspace, List<Row> viewRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, VersionNumber cassandraVersion) {
        Map<String, MaterializedViewMetadata> views = new LinkedHashMap<String, MaterializedViewMetadata>();
        if (viewRows != null) {
            for (Row viewRow : viewRows) {
                String viewName = viewRow.getString("view_name");
                try {
                    Map<String, ColumnMetadata.Raw> cols = colsDefs.get(viewName);
                    if (cols == null || cols.isEmpty())
                        continue; // we probably raced, we will update the metadata next time

                    MaterializedViewMetadata view = MaterializedViewMetadata.build(keyspace, viewRow, cols, cassandraVersion);
                    if(view != null)
                        views.put(view.getName(), view);
                } catch (RuntimeException e) {
                    // See ControlConnection#refreshSchema for why we'd rather not probably this further
                    logger.error(String.format("Error parsing schema for view %s.%s: "
                            + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\") will be missing or incomplete",
                        keyspace.getName(), viewName, keyspace.getName(), viewName), e);
                }
            }
        }
        return views;
    }

    // Update oldKeyspaces with the changes contained in newKeyspaces.
    // This method also takes care of triggering the relevant events
    private void updateKeyspaces(Metadata metadata, Map<String, KeyspaceMetadata> oldKeyspaces, Map<String, KeyspaceMetadata> newKeyspaces, String keyspaceToRebuild) {
        Iterator<KeyspaceMetadata> it = oldKeyspaces.values().iterator();
        while (it.hasNext()) {
            KeyspaceMetadata oldKeyspace = it.next();
            String keyspaceName = oldKeyspace.getName();
            // If we're rebuilding only a single keyspace, we should only consider that one
            // because newKeyspaces will only contain that keyspace.
            if ((keyspaceToRebuild == null || keyspaceToRebuild.equals(keyspaceName)) && !newKeyspaces.containsKey(keyspaceName)) {
                it.remove();
                metadata.triggerOnKeyspaceRemoved(oldKeyspace);
            }
        }
        for (KeyspaceMetadata newKeyspace : newKeyspaces.values()) {
            KeyspaceMetadata oldKeyspace = oldKeyspaces.put(newKeyspace.getName(), newKeyspace);
            if (oldKeyspace == null) {
                metadata.triggerOnKeyspaceAdded(newKeyspace);
            } else if (!oldKeyspace.equals(newKeyspace)) {
                metadata.triggerOnKeyspaceChanged(newKeyspace, oldKeyspace);
            }
            Map<String, TableMetadata> oldTables = oldKeyspace == null ? new HashMap<String, TableMetadata>() : oldKeyspace.tables;
            updateTables(metadata, oldTables, newKeyspace.tables, null);
            Map<String, UserType> oldTypes = oldKeyspace == null ? new HashMap<String, UserType>() : oldKeyspace.userTypes;
            updateUserTypes(metadata, oldTypes, newKeyspace.userTypes, null);
            Map<String, FunctionMetadata> oldFunctions = oldKeyspace == null ? new HashMap<String, FunctionMetadata>() : oldKeyspace.functions;
            updateFunctions(metadata, oldFunctions, newKeyspace.functions, null);
            Map<String, AggregateMetadata> oldAggregates = oldKeyspace == null ? new HashMap<String, AggregateMetadata>() : oldKeyspace.aggregates;
            updateAggregates(metadata, oldAggregates, newKeyspace.aggregates, null);
            Map<String, MaterializedViewMetadata> oldViews = oldKeyspace == null ? new HashMap<String, MaterializedViewMetadata>() : oldKeyspace.views;
            updateViews(metadata, oldViews, newKeyspace.views, null);
        }
    }

    private void updateTables(Metadata metadata, Map<String, TableMetadata> oldTables, Map<String, TableMetadata> newTables, String tableToRebuild) {
        Iterator<TableMetadata> it = oldTables.values().iterator();
        while (it.hasNext()) {
            TableMetadata oldTable = it.next();
            String tableName = oldTable.getName();
            // If we're rebuilding only a single table, we should only consider that one
            // because newTables will only contain that table.
            if ((tableToRebuild == null || tableToRebuild.equals(tableName)) && !newTables.containsKey(tableName)) {
                it.remove();
                metadata.triggerOnTableRemoved(oldTable);
            }
        }
        for (TableMetadata newTable : newTables.values()) {
            TableMetadata oldTable = oldTables.put(newTable.getName(), newTable);
            if (oldTable == null) {
                metadata.triggerOnTableAdded(newTable);
            } else if (!oldTable.equals(newTable)) {
                metadata.triggerOnTableChanged(newTable, oldTable);
            }
        }
    }

    private void updateUserTypes(Metadata metadata, Map<String, UserType> oldTypes, Map<String, UserType> newTypes, String typeToRebuild) {
        Iterator<UserType> it = oldTypes.values().iterator();
        while (it.hasNext()) {
            UserType oldType = it.next();
            String typeName = oldType.getTypeName();
            if ((typeToRebuild == null || typeToRebuild.equals(typeName)) && !newTypes.containsKey(typeName)) {
                it.remove();
                metadata.triggerOnUserTypeRemoved(oldType);
            }
        }
        for (UserType newType : newTypes.values()) {
            UserType oldType = oldTypes.put(newType.getTypeName(), newType);
            if (oldType == null) {
                metadata.triggerOnUserTypeAdded(newType);
            } else if (!newType.equals(oldType)) {
                metadata.triggerOnUserTypeChanged(newType, oldType);
            }
        }
    }

    private void updateFunctions(Metadata metadata, Map<String, FunctionMetadata> oldFunctions, Map<String, FunctionMetadata> newFunctions, String functionToRebuild) {
        Iterator<FunctionMetadata> it = oldFunctions.values().iterator();
        while (it.hasNext()) {
            FunctionMetadata oldFunction = it.next();
            String functionName = oldFunction.getFullName();
            if ((functionToRebuild == null || functionToRebuild.equals(functionName)) && !newFunctions.containsKey(functionName)) {
                it.remove();
                metadata.triggerOnFunctionRemoved(oldFunction);
            }
        }
        for (FunctionMetadata newFunction : newFunctions.values()) {
            FunctionMetadata oldFunction = oldFunctions.put(newFunction.getFullName(), newFunction);
            if (oldFunction == null) {
                metadata.triggerOnFunctionAdded(newFunction);
            } else if (!newFunction.equals(oldFunction)) {
                metadata.triggerOnFunctionChanged(newFunction, oldFunction);
            }
        }
    }

    private void updateAggregates(Metadata metadata, Map<String, AggregateMetadata> oldAggregates, Map<String, AggregateMetadata> newAggregates, String aggregateToRebuild) {
        Iterator<AggregateMetadata> it = oldAggregates.values().iterator();
        while (it.hasNext()) {
            AggregateMetadata oldAggregate = it.next();
            String aggregateName = oldAggregate.getFullName();
            if ((aggregateToRebuild == null || aggregateToRebuild.equals(aggregateName)) && !newAggregates.containsKey(aggregateName)) {
                it.remove();
                metadata.triggerOnAggregateRemoved(oldAggregate);
            }
        }
        for (AggregateMetadata newAggregate : newAggregates.values()) {
            AggregateMetadata oldAggregate = oldAggregates.put(newAggregate.getFullName(), newAggregate);
            if (oldAggregate == null) {
                metadata.triggerOnAggregateAdded(newAggregate);
            } else if (!newAggregate.equals(oldAggregate)) {
                metadata.triggerOnAggregateChanged(newAggregate, oldAggregate);
            }
        }
    }

    private void updateViews(Metadata metadata, Map<String, MaterializedViewMetadata> oldViews, Map<String, MaterializedViewMetadata> newViews, String viewToRebuild) {
        Iterator<MaterializedViewMetadata> it = oldViews.values().iterator();
        while (it.hasNext()) {
            MaterializedViewMetadata oldView = it.next();
            String aggregateName = oldView.getName();
            if ((viewToRebuild == null || viewToRebuild.equals(aggregateName)) && !newViews.containsKey(aggregateName)) {
                it.remove();
                metadata.triggerOnMaterializedViewRemoved(oldView);
            }
        }
        for (MaterializedViewMetadata newView : newViews.values()) {
            MaterializedViewMetadata oldView = oldViews.put(newView.getName(), newView);
            if (oldView == null) {
                metadata.triggerOnMaterializedViewAdded(newView);
            } else if (!newView.equals(oldView)) {
                metadata.triggerOnMaterializedViewChanged(newView, oldView);
            }
        }
    }

    static Map<String, List<Row>> groupByKeyspace(ResultSet rs) {
        if (rs == null)
            return Collections.emptyMap();

        Map<String, List<Row>> result = new HashMap<String, List<Row>>();
        for (Row row : rs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            List<Row> l = result.get(ksName);
            if (l == null) {
                l = new ArrayList<Row>();
                result.put(ksName, l);
            }
            l.add(row);
        }
        return result;
    }

    static Map<String, Map<String, List<Row>>> groupByKeyspaceAndCf(ResultSet rs, String tableName) {
        if (rs == null)
            return Collections.emptyMap();

        Map<String, Map<String, List<Row>>> result = Maps.newHashMap();
        for (Row row : rs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            String cfName = row.getString(tableName);
            Map<String, List<Row>> rowsByCf = result.get(ksName);
            if (rowsByCf == null) {
                rowsByCf = Maps.newHashMap();
                result.put(ksName, rowsByCf);
            }
            List<Row> l = rowsByCf.get(cfName);
            if (l == null) {
                l = Lists.newArrayList();
                rowsByCf.put(cfName, l);
            }
            l.add(row);
        }
        return result;
    }

    static Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> groupByKeyspaceAndCf(ResultSet rs, VersionNumber cassandraVersion, ProtocolVersion protocolVersion, CodecRegistry codecRegistry, String tableName) {
        if (rs == null)
            return Collections.emptyMap();

        Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> result =
            new HashMap<String, Map<String, Map<String, ColumnMetadata.Raw>>>();
        for (Row row : rs) {
            String ksName = row.getString(KeyspaceMetadata.KS_NAME);
            String cfName = row.getString(tableName);
            Map<String, Map<String, ColumnMetadata.Raw>> colsByCf = result.get(ksName);
            if (colsByCf == null) {
                colsByCf = new HashMap<String, Map<String, ColumnMetadata.Raw>>();
                result.put(ksName, colsByCf);
            }
            Map<String, ColumnMetadata.Raw> l = colsByCf.get(cfName);
            if (l == null) {
                l = new HashMap<String, ColumnMetadata.Raw>();
                colsByCf.put(cfName, l);
            }
            ColumnMetadata.Raw c = ColumnMetadata.Raw.fromRow(row, cassandraVersion, protocolVersion, codecRegistry);
            l.put(c.name, c);
        }
        return result;
    }

    private static ResultSetFuture queryAsync(String query, Connection connection, ProtocolVersion protocolVersion) throws ConnectionException, BusyConnectionException {
        DefaultResultSetFuture future = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query(query));
        connection.write(future);
        return future;
    }

    private static ResultSet get(ResultSetFuture future) throws InterruptedException, ExecutionException {
        return (future == null) ? null : future.get();
    }

    /**
     * The rows from the system tables that we want to parse to metadata classes.
     * The format of these rows depends on the Cassandra version, but our parsing code knows how to handle the differences.
     */
    private static class SystemRows {
        final ResultSet keyspaces;
        final Map<String, List<Row>> tables;
        final Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> columns;
        final Map<String, List<Row>> udts;
        final Map<String, List<Row>> functions;
        final Map<String, List<Row>> aggregates;
        final Map<String, List<Row>> views;
        final Map<String, Map<String, List<Row>>> indexes;

        public SystemRows(ResultSet keyspaces, Map<String, List<Row>> tables, Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> columns, Map<String, List<Row>> udts, Map<String, List<Row>> functions,
                          Map<String, List<Row>> aggregates, Map<String, List<Row>> views, Map<String, Map<String, List<Row>>> indexes) {
            this.keyspaces = keyspaces;
            this.tables = tables;
            this.columns = columns;
            this.udts = udts;
            this.functions = functions;
            this.aggregates = aggregates;
            this.views = views;
            this.indexes = indexes;
        }
    }

    private static class V2SchemaParser extends SchemaParser {

        private static final String SELECT_KEYSPACES       = "SELECT * FROM system.schema_keyspaces";
        private static final String SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies";
        private static final String SELECT_COLUMNS         = "SELECT * FROM system.schema_columns";
        private static final String SELECT_USERTYPES       = "SELECT * FROM system.schema_usertypes";
        private static final String SELECT_FUNCTIONS       = "SELECT * FROM system.schema_functions";
        private static final String SELECT_AGGREGATES      = "SELECT * FROM system.schema_aggregates";

        private static final String CF_NAME                = "columnfamily_name";

        @Override
        SystemRows fetchSystemRows(Metadata metadata,
                                   SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                                   Connection connection, VersionNumber cassandraVersion)
            throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

            boolean isSchemaOrKeyspace = (targetType == null || targetType == KEYSPACE);
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();
            CodecRegistry codecRegistry = metadata.cluster.configuration.getCodecRegistry();

            String whereClause = "";
            if (targetType != null) {
                whereClause = " WHERE keyspace_name = '" + targetKeyspace + '\'';
                if (targetType == TABLE)
                    whereClause += " AND columnfamily_name = '" + targetName + '\'';
                else if (targetType == TYPE)
                    whereClause += " AND type_name = '" + targetName + '\'';
                else if (targetType == FUNCTION)
                    whereClause += " AND function_name = '" + targetName + "' AND signature = " + LIST_OF_TEXT_CODEC.format(targetSignature);
                else if (targetType == AGGREGATE)
                    whereClause += " AND aggregate_name = '" + targetName + "' AND signature = " + LIST_OF_TEXT_CODEC.format(targetSignature);
            }

            ResultSetFuture ksFuture = null,
                udtFuture = null,
                cfFuture = null,
                colsFuture = null,
                functionsFuture = null,
                aggregatesFuture = null;

            if (isSchemaOrKeyspace)
                ksFuture = queryAsync(SELECT_KEYSPACES + whereClause, connection, protocolVersion);

            if (isSchemaOrKeyspace && supportsUdts(cassandraVersion) || targetType == TYPE)
                udtFuture = queryAsync(SELECT_USERTYPES + whereClause, connection, protocolVersion);

            if (isSchemaOrKeyspace || targetType == TABLE) {
                cfFuture = queryAsync(SELECT_COLUMN_FAMILIES + whereClause, connection, protocolVersion);
                colsFuture = queryAsync(SELECT_COLUMNS + whereClause, connection, protocolVersion);
            }

            if ((isSchemaOrKeyspace && supportsUdfs(cassandraVersion) || targetType == FUNCTION))
                functionsFuture = queryAsync(SELECT_FUNCTIONS + whereClause, connection, protocolVersion);

            if (isSchemaOrKeyspace && supportsUdfs(cassandraVersion) || targetType == AGGREGATE)
                aggregatesFuture = queryAsync(SELECT_AGGREGATES + whereClause, connection, protocolVersion);

            return new SystemRows(get(ksFuture),
                groupByKeyspace(get(cfFuture)),
                groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, protocolVersion, codecRegistry, CF_NAME),
                groupByKeyspace(get(udtFuture)),
                groupByKeyspace(get(functionsFuture)),
                groupByKeyspace(get(aggregatesFuture)),
                // No views nor separate indexes table in Cassandra 2:
                Collections.<String, List<Row>>emptyMap(),
                Collections.<String, Map<String, List<Row>>>emptyMap());
        }

        @Override
        String tableNameColumn() {
            return CF_NAME;
        }

        private boolean supportsUdts(VersionNumber cassandraVersion) {
            return cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1);
        }

        private boolean supportsUdfs(VersionNumber cassandraVersion) {
            return cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 2);
        }

    }

    private static class V3SchemaParser extends SchemaParser {

        private static final String SELECT_KEYSPACES       = "SELECT * FROM system_schema.keyspaces";
        private static final String SELECT_TABLES          = "SELECT * FROM system_schema.tables";
        private static final String SELECT_COLUMNS         = "SELECT * FROM system_schema.columns";
        private static final String SELECT_USERTYPES       = "SELECT * FROM system_schema.types";
        private static final String SELECT_FUNCTIONS       = "SELECT * FROM system_schema.functions";
        private static final String SELECT_AGGREGATES      = "SELECT * FROM system_schema.aggregates";
        private static final String SELECT_INDEXES         = "SELECT * FROM system_schema.indexes";
        private static final String SELECT_VIEWS           = "SELECT * FROM system_schema.views";

        private static final String TABLE_NAME = "table_name";

        @Override
        SystemRows fetchSystemRows(Metadata metadata, SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature, Connection connection, VersionNumber cassandraVersion)
            throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

            boolean isSchemaOrKeyspace = (targetType == null || targetType == KEYSPACE);
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();
            CodecRegistry codecRegistry = metadata.cluster.configuration.getCodecRegistry();

            ResultSetFuture ksFuture = null,
                udtFuture = null,
                cfFuture = null,
                colsFuture = null,
                functionsFuture = null,
                aggregatesFuture = null,
                indexesFuture = null,
                viewsFuture = null;

            if (isSchemaOrKeyspace)
                ksFuture = queryAsync(SELECT_KEYSPACES + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);

            if (isSchemaOrKeyspace || targetType == TYPE)
                udtFuture = queryAsync(SELECT_USERTYPES + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);

            if (isSchemaOrKeyspace || targetType == TABLE) {
                cfFuture = queryAsync(SELECT_TABLES + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);
                colsFuture = queryAsync(SELECT_COLUMNS + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);
                indexesFuture = queryAsync(SELECT_INDEXES + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);
                viewsFuture = queryAsync(SELECT_VIEWS + whereClause(targetType == TABLE ? VIEW : targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);
            }

            if (isSchemaOrKeyspace || targetType == FUNCTION)
                functionsFuture = queryAsync(SELECT_FUNCTIONS + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);

            if (isSchemaOrKeyspace || targetType == AGGREGATE)
                aggregatesFuture = queryAsync(SELECT_AGGREGATES + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);

            return new SystemRows(get(ksFuture),
                groupByKeyspace(get(cfFuture)),
                groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, protocolVersion, codecRegistry, TABLE_NAME),
                groupByKeyspace(get(udtFuture)),
                groupByKeyspace(get(functionsFuture)),
                groupByKeyspace(get(aggregatesFuture)),
                groupByKeyspace(get(viewsFuture)),
                groupByKeyspaceAndCf(get(indexesFuture), TABLE_NAME));
        }

        @Override
        String tableNameColumn() {
            return TABLE_NAME;
        }

        private String whereClause(SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature) {
            String whereClause = "";
            if (targetType != null) {
                whereClause = " WHERE keyspace_name = '" + targetKeyspace + '\'';
                if (targetType == TABLE)
                    whereClause += " AND table_name = '" + targetName + '\'';
                else if (targetType == VIEW)
                    whereClause += " AND view_name = '" + targetName + '\'';
                else if (targetType == TYPE)
                    whereClause += " AND type_name = '" + targetName + '\'';
                else if (targetType == FUNCTION)
                    whereClause += " AND function_name = '" + targetName + "' AND signature = " + LIST_OF_TEXT_CODEC.format(targetSignature);
                else if (targetType == AGGREGATE)
                    whereClause += " AND aggregate_name = '" + targetName + "' AND signature = " + LIST_OF_TEXT_CODEC.format(targetSignature);
            }
            return whereClause;
        }

    }
}
