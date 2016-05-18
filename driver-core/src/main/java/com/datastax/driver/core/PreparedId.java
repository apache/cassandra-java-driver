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

import java.util.List;

import static com.datastax.driver.core.ProtocolVersion.V4;

/**
 * Identifies a PreparedStatement.
 */
// This class is mostly here to group PreparedStatement data that are need for
// execution but that we don't want to expose publicly (see JAVA-195)
public class PreparedId {

    private volatile MD5Digest id;
    private volatile ColumnDefinitions variables;
    private volatile ColumnDefinitions resultSetMetadata;
    private volatile int[] routingKeyIndexes;
    private volatile ProtocolVersion protocolVersion;

    private PreparedId(MD5Digest id, ColumnDefinitions variables, ColumnDefinitions resultSetMetadata, int[] routingKeyIndexes, ProtocolVersion protocolVersion) {
        this.id = id;
        this.variables = variables;
        this.resultSetMetadata = resultSetMetadata;
        this.routingKeyIndexes = routingKeyIndexes;
        this.protocolVersion = protocolVersion;
    }

    MD5Digest getId() {
        return id;
    }

    /**
     * Returns the metadata about bound variables in the statement.
     *
     * @return the metadata about bound variables in the statement.
     */
    ColumnDefinitions getVariables() {
        return variables;
    }

    /**
     * Returns the metadata about columns returned by the statement.
     *
     * @return the metadata about columns returned by the statement.
     */
    ColumnDefinitions getResultSetMetadata() {
        return resultSetMetadata;
    }

    int[] getRoutingKeyIndexes() {
        return routingKeyIndexes;
    }

    ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    void swap(PreparedId updated) {
        this.id = updated.id;
        this.variables = updated.variables;
        this.resultSetMetadata = updated.resultSetMetadata;
        this.routingKeyIndexes = updated.routingKeyIndexes;
        this.protocolVersion = updated.protocolVersion;
    }

    static PreparedId fromMessage(Responses.Result.Prepared msg, Cluster cluster) {
        ColumnDefinitions defs = msg.metadata.columns;
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        if (defs.size() == 0) {
            return new PreparedId(msg.statementId, defs, msg.resultMetadata.columns, null, protocolVersion);
        } else {
            int[] pkIndices = (protocolVersion.compareTo(V4) >= 0)
                    ? msg.metadata.pkIndices
                    : computePkIndices(cluster.getMetadata(), defs);
            return new PreparedId(msg.statementId, defs, msg.resultMetadata.columns, pkIndices, protocolVersion);
        }
    }

    private static int[] computePkIndices(Metadata clusterMetadata, ColumnDefinitions boundColumns) {
        List<ColumnMetadata> partitionKeyColumns = null;
        int[] pkIndexes = null;
        KeyspaceMetadata km = clusterMetadata.getKeyspace(Metadata.quote(boundColumns.getKeyspace(0)));
        if (km != null) {
            TableMetadata tm = km.getTable(Metadata.quote(boundColumns.getTable(0)));
            if (tm != null) {
                partitionKeyColumns = tm.getPartitionKey();
                pkIndexes = new int[partitionKeyColumns.size()];
                for (int i = 0; i < pkIndexes.length; ++i)
                    pkIndexes[i] = -1;
            }
        }

        // Note: we rely on the fact CQL queries cannot span multiple tables. If that change, we'll have to get smarter.
        for (int i = 0; i < boundColumns.size(); i++)
            maybeGetIndex(boundColumns.getName(i), i, partitionKeyColumns, pkIndexes);

        return allSet(pkIndexes) ? pkIndexes : null;
    }

    private static void maybeGetIndex(String name, int j, List<ColumnMetadata> pkColumns, int[] pkIndexes) {
        if (pkColumns == null)
            return;

        for (int i = 0; i < pkColumns.size(); ++i) {
            if (name.equals(pkColumns.get(i).getName())) {
                // We may have the same column prepared multiple times, but only pick the first value
                pkIndexes[i] = j;
                return;
            }
        }
    }

    private static boolean allSet(int[] pkColumns) {
        if (pkColumns == null)
            return false;

        for (int i = 0; i < pkColumns.length; ++i)
            if (pkColumns[i] < 0)
                return false;

        return true;
    }

}
