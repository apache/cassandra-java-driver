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

    static SchemaParser forVersion(VersionNumber cassandraVersion) {
        if(cassandraVersion.getMajor() >= 3) return V3_PARSER;
        return V2_PARSER;
    }

    abstract void refresh(Metadata metadata,
                          SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                          Connection connection, VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException;

    private static SchemaParser V2_PARSER = new SchemaParser() {

        private static final String SELECT_KEYSPACES       = "SELECT * FROM system.schema_keyspaces";
        private static final String SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies";
        private static final String SELECT_COLUMNS         = "SELECT * FROM system.schema_columns";
        private static final String SELECT_USERTYPES       = "SELECT * FROM system.schema_usertypes";
        private static final String SELECT_FUNCTIONS       = "SELECT * FROM system.schema_functions";
        private static final String SELECT_AGGREGATES      = "SELECT * FROM system.schema_aggregates";

        private static final String CF_NAME                = "columnfamily_name";

        @Override
        void refresh(Metadata metadata,
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

            ResultSet ks = get(ksFuture);
            Map<String, List<Row>> udtDefs = groupByKeyspace(get(udtFuture));
            Map<String, List<Row>> cfDefs = groupByKeyspace(get(cfFuture));
            Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs = groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, protocolVersion, codecRegistry, CF_NAME);
            Map<String, List<Row>> functionDefs = groupByKeyspace(get(functionsFuture));
            Map<String, List<Row>> aggregateDefs = groupByKeyspace(get(aggregatesFuture));

            metadata.lock.lock();
            try {
                if (targetType == null || targetType == KEYSPACE) { // Refresh one or all keyspaces
                    assert ks != null;
                    Set<String> addedKs = new HashSet<String>();
                    for (Row ksRow : ks) {
                        String ksName = ksRow.getString(KeyspaceMetadata.KS_NAME);
                        KeyspaceMetadata ksm = KeyspaceMetadata.buildV2(ksRow);

                        if (udtDefs.containsKey(ksName)) {
                            buildUserTypes(ksm, udtDefs.get(ksName), protocolVersion, codecRegistry);
                        }
                        if (cfDefs.containsKey(ksName)) {
                            buildTableMetadata(ksm, cfDefs.get(ksName), colsDefs.get(ksName), null, cassandraVersion, protocolVersion, codecRegistry, CF_NAME);
                        }
                        if (functionDefs.containsKey(ksName)) {
                            buildFunctionMetadata(ksm, functionDefs.get(ksName), protocolVersion, codecRegistry);
                        }
                        if (aggregateDefs.containsKey(ksName)) {
                            buildAggregateMetadata(ksm, aggregateDefs.get(ksName), protocolVersion, codecRegistry);
                        }
                        addedKs.add(ksName);
                        metadata.keyspaces.put(ksName, ksm);
                    }

                    // If keyspace is null, it means we're rebuilding from scratch, so
                    // remove anything that was not just added as it means it's a dropped keyspace
                    if (targetKeyspace == null) {
                        Iterator<String> iter = metadata.keyspaces.keySet().iterator();
                        while (iter.hasNext()) {
                            if (!addedKs.contains(iter.next()))
                                iter.remove();
                        }
                    }
                } else {
                    assert targetKeyspace != null;
                    KeyspaceMetadata ksm = metadata.keyspaces.get(targetKeyspace);

                    // If we update a keyspace we don't know about, something went
                    // wrong. Log an error an schedule a full schema rebuilt.
                    if (ksm == null) {
                        logger.error(String.format("Asked to rebuild %s %s.%s but I don't know keyspace %s", targetType, targetKeyspace, targetName, targetKeyspace));
                        metadata.cluster.submitSchemaRefresh(null, null, null, null);
                        return;
                    }

                    switch (targetType) {
                        case TABLE:
                            if (cfDefs.containsKey(targetKeyspace))
                                buildTableMetadata(ksm, cfDefs.get(targetKeyspace), colsDefs.get(targetKeyspace), null, cassandraVersion, protocolVersion, codecRegistry, CF_NAME);
                            break;
                        case TYPE:
                            if (udtDefs.containsKey(targetKeyspace))
                                buildUserTypes(ksm, udtDefs.get(targetKeyspace), protocolVersion, codecRegistry);
                            break;
                        case FUNCTION:
                            if (functionDefs.containsKey(targetKeyspace))
                                buildFunctionMetadata(ksm, functionDefs.get(targetKeyspace), protocolVersion, codecRegistry);
                            break;
                        case AGGREGATE:
                            if (functionDefs.containsKey(targetKeyspace))
                                buildAggregateMetadata(ksm, aggregateDefs.get(targetKeyspace), protocolVersion, codecRegistry);
                            break;
                        default:
                            logger.warn("Unexpected element type to rebuild: {}", targetType);
                    }
                }
            } finally {
                metadata.lock.unlock();
            }
        }

        private boolean supportsUdts(VersionNumber cassandraVersion) {
            return cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1);
        }

        private boolean supportsUdfs(VersionNumber cassandraVersion) {
            return cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 2);
        }

    };

    private static SchemaParser V3_PARSER = new SchemaParser() {

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
        void refresh(Metadata metadata,
                     SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                     Connection connection, VersionNumber cassandraVersion)
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
                viewsFuture = queryAsync(SELECT_VIEWS + whereClause(targetType == null ? null : VIEW, targetKeyspace, targetName, targetSignature), connection, protocolVersion);
            }

            if (isSchemaOrKeyspace || targetType == FUNCTION)
                functionsFuture = queryAsync(SELECT_FUNCTIONS + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);

            if (isSchemaOrKeyspace || targetType == AGGREGATE)
                aggregatesFuture = queryAsync(SELECT_AGGREGATES + whereClause(targetType, targetKeyspace, targetName, targetSignature), connection, protocolVersion);

            ResultSet ks = get(ksFuture);
            Map<String, List<Row>> udtDefs = groupByKeyspace(get(udtFuture));
            Map<String, List<Row>> cfDefs = groupByKeyspace(get(cfFuture));
            Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs = groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, protocolVersion, codecRegistry, TABLE_NAME);
            Map<String, List<Row>> functionDefs = groupByKeyspace(get(functionsFuture));
            Map<String, List<Row>> aggregateDefs = groupByKeyspace(get(aggregatesFuture));
            Map<String, List<Row>> viewDefs = groupByKeyspace(get(viewsFuture));
            Map<String, Map<String, List<Row>>> indexDefs = groupByKeyspaceAndCf(get(indexesFuture), TABLE_NAME);

            metadata.lock.lock();
            try {
                if (targetType == null || targetType == KEYSPACE) { // Refresh one or all keyspaces
                    assert ks != null;
                    Set<String> addedKs = new HashSet<String>();
                    for (Row ksRow : ks) {
                        String ksName = ksRow.getString(KeyspaceMetadata.KS_NAME);
                        KeyspaceMetadata ksm = KeyspaceMetadata.buildV3(ksRow);

                        if (udtDefs.containsKey(ksName)) {
                            buildUserTypes(ksm, udtDefs.get(ksName), protocolVersion, codecRegistry);
                        }
                        if (cfDefs.containsKey(ksName)) {
                            buildTableMetadata(ksm, cfDefs.get(ksName), colsDefs.get(ksName), indexDefs.get(ksName), cassandraVersion, protocolVersion, codecRegistry, TABLE_NAME);
                        }
                        if (viewDefs.containsKey(ksName)) {
                            buildViewMetadata(ksm, viewDefs.get(ksName), colsDefs.get(ksName), cassandraVersion);
                        }
                        if (functionDefs.containsKey(ksName)) {
                            buildFunctionMetadata(ksm, functionDefs.get(ksName), protocolVersion, codecRegistry);
                        }
                        if (aggregateDefs.containsKey(ksName)) {
                            buildAggregateMetadata(ksm, aggregateDefs.get(ksName), protocolVersion, codecRegistry);
                        }
                        addedKs.add(ksName);
                        metadata.keyspaces.put(ksName, ksm);
                    }

                    // If keyspace is null, it means we're rebuilding from scratch, so
                    // remove anything that was not just added as it means it's a dropped keyspace
                    if (targetKeyspace == null) {
                        Iterator<String> iter = metadata.keyspaces.keySet().iterator();
                        while (iter.hasNext()) {
                            if (!addedKs.contains(iter.next()))
                                iter.remove();
                        }
                    }
                } else {
                    assert targetKeyspace != null;
                    KeyspaceMetadata ksm = metadata.keyspaces.get(targetKeyspace);

                    // If we update a keyspace we don't know about, something went
                    // wrong. Log an error an schedule a full schema rebuilt.
                    if (ksm == null) {
                        logger.error(String.format("Asked to rebuild %s %s.%s but I don't know keyspace %s", targetType, targetKeyspace, targetName, targetKeyspace));
                        metadata.cluster.submitSchemaRefresh(null, null, null, null);
                        return;
                    }

                    switch (targetType) {
                        case TABLE:
                            if (cfDefs.containsKey(targetKeyspace))
                                buildTableMetadata(ksm, cfDefs.get(targetKeyspace), colsDefs.get(targetKeyspace), indexDefs.get(targetKeyspace), cassandraVersion, protocolVersion, codecRegistry, TABLE_NAME);
                            if (viewDefs.containsKey(targetKeyspace))
                                buildViewMetadata(ksm, viewDefs.get(targetKeyspace), colsDefs.get(targetKeyspace), cassandraVersion);
                            break;
                        case TYPE:
                            if (udtDefs.containsKey(targetKeyspace))
                                buildUserTypes(ksm, udtDefs.get(targetKeyspace), protocolVersion, codecRegistry);
                            break;
                        case FUNCTION:
                            if (functionDefs.containsKey(targetKeyspace))
                                buildFunctionMetadata(ksm, functionDefs.get(targetKeyspace), protocolVersion, codecRegistry);
                            break;
                        case AGGREGATE:
                            if (functionDefs.containsKey(targetKeyspace))
                                buildAggregateMetadata(ksm, aggregateDefs.get(targetKeyspace), protocolVersion, codecRegistry);
                            break;
                        default:
                            logger.warn("Unexpected element type to rebuild: {}", targetType);
                    }
                }
            } finally {
                metadata.lock.unlock();
            }
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

    };

    static void buildTableMetadata(KeyspaceMetadata ksm, List<Row> cfRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, Map<String, List<Row>> ksIndexes, VersionNumber cassandraVersion, ProtocolVersion protocolVersion, CodecRegistry codecRegistry, String tableName) {
        for (Row cfRow : cfRows) {
            String cfName = cfRow.getString(tableName);
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
                        // C* 1.2 don't persists default CQL metadata, so it's possible not to have columns (for thrift
                        // tables). But in that case TableMetadata.build() knows how to handle it.
                        cols = Collections.emptyMap();
                    }
                }
                List<Row> cfIndexes = (ksIndexes == null) ? null : ksIndexes.get(cfName);
                ksm.add(TableMetadata.build(ksm, cfRow, cols, cfIndexes, tableName, cassandraVersion, protocolVersion, codecRegistry));
            } catch (RuntimeException e) {
            // See ControlConnection#refreshSchema for why we'd rather not probably this further
            logger.error(String.format("Error parsing schema for table %s.%s: "
                    + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\") will be missing or incomplete",
                ksm.getName(), cfName, ksm.getName(), cfName), e);
            }
        }
    }

    static void buildViewMetadata(KeyspaceMetadata ksm, List<Row> viewRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, VersionNumber cassandraVersion) {
        for (Row viewRow : viewRows) {
            String viewName = viewRow.getString("view_name");
            try {
                Map<String, ColumnMetadata.Raw> cols = colsDefs.get(viewName);
                if (cols == null || cols.isEmpty())
                    continue; // we probably raced, we will update the metadata next time
                MaterializedViewMetadata view = MaterializedViewMetadata.build(ksm, viewRow, cols, cassandraVersion);
                if(view != null)
                    ksm.add(view);
            } catch (RuntimeException e) {
                // See ControlConnection#refreshSchema for why we'd rather not probably this further
                logger.error(String.format("Error parsing schema for view %s.%s: "
                        + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\") will be missing or incomplete",
                    ksm.getName(), viewName, ksm.getName(), viewName), e);
            }
        }
    }
    static void buildUserTypes(KeyspaceMetadata ksm, List<Row> rows, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        for (Row row : rows)
            ksm.add(UserType.build(row, protocolVersion, codecRegistry));
    }

    static void buildFunctionMetadata(KeyspaceMetadata ksm, List<Row> rows, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        for (Row row : rows)
            ksm.add(FunctionMetadata.build(ksm, row, protocolVersion, codecRegistry));
    }

    static void buildAggregateMetadata(KeyspaceMetadata ksm, List<Row> rows, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        for (Row row : rows)
            ksm.add(AggregateMetadata.build(ksm, row, protocolVersion, codecRegistry));
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

}
