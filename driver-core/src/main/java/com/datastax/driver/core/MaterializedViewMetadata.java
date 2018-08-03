/*
 * Copyright DataStax, Inc.
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

import com.datastax.driver.core.utils.MoreObjects;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An immutable representation of a materialized view. Materialized views are available starting
 * from Cassandra 3.0.
 */
public class MaterializedViewMetadata extends AbstractTableMetadata {

  private static final Logger logger = LoggerFactory.getLogger(MaterializedViewMetadata.class);

  private volatile TableMetadata baseTable;

  private final boolean includeAllColumns;

  private final String whereClause;

  private MaterializedViewMetadata(
      KeyspaceMetadata keyspace,
      TableMetadata baseTable,
      String name,
      UUID id,
      List<ColumnMetadata> partitionKey,
      List<ColumnMetadata> clusteringColumns,
      Map<String, ColumnMetadata> columns,
      boolean includeAllColumns,
      String whereClause,
      TableOptionsMetadata options,
      List<ClusteringOrder> clusteringOrder,
      VersionNumber cassandraVersion) {
    super(
        keyspace,
        name,
        id,
        partitionKey,
        clusteringColumns,
        columns,
        options,
        clusteringOrder,
        cassandraVersion);
    this.baseTable = baseTable;
    this.includeAllColumns = includeAllColumns;
    this.whereClause = whereClause;
  }

  static MaterializedViewMetadata build(
      KeyspaceMetadata keyspace,
      Row row,
      Map<String, ColumnMetadata.Raw> rawCols,
      VersionNumber cassandraVersion,
      Cluster cluster) {

    String name = row.getString("view_name");
    String tableName = row.getString("base_table_name");
    TableMetadata baseTable = keyspace.tables.get(tableName);
    if (baseTable == null) {
      logger.trace(
          String.format(
              "Cannot find base table %s for materialized view %s.%s: "
                  + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\") will return null",
              tableName, keyspace.getName(), name, keyspace.getName(), name));
      return null;
    }

    UUID id = row.getUUID("id");
    boolean includeAllColumns = row.getBool("include_all_columns");
    String whereClause = row.getString("where_clause");

    int partitionKeySize =
        findCollectionSize(rawCols.values(), ColumnMetadata.Raw.Kind.PARTITION_KEY);
    int clusteringSize =
        findCollectionSize(rawCols.values(), ColumnMetadata.Raw.Kind.CLUSTERING_COLUMN);

    List<ColumnMetadata> partitionKey =
        new ArrayList<ColumnMetadata>(Collections.<ColumnMetadata>nCopies(partitionKeySize, null));
    List<ColumnMetadata> clusteringColumns =
        new ArrayList<ColumnMetadata>(Collections.<ColumnMetadata>nCopies(clusteringSize, null));
    List<ClusteringOrder> clusteringOrder =
        new ArrayList<ClusteringOrder>(Collections.<ClusteringOrder>nCopies(clusteringSize, null));

    // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
    LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();

    TableOptionsMetadata options = null;
    try {
      options = new TableOptionsMetadata(row, false, cassandraVersion);
    } catch (RuntimeException e) {
      // See ControlConnection#refreshSchema for why we'd rather not probably this further. Since
      // table options is one thing
      // that tends to change often in Cassandra, it's worth special casing this.
      logger.error(
          String.format(
              "Error parsing schema options for view %s.%s: "
                  + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\").getOptions() will return null",
              keyspace.getName(), name, keyspace.getName(), name),
          e);
    }

    MaterializedViewMetadata view =
        new MaterializedViewMetadata(
            keyspace,
            baseTable,
            name,
            id,
            partitionKey,
            clusteringColumns,
            columns,
            includeAllColumns,
            whereClause,
            options,
            clusteringOrder,
            cassandraVersion);

    // We use this temporary set just so non PK columns are added in lexicographical order, which is
    // the one of a
    // 'SELECT * FROM ...'
    Set<ColumnMetadata> otherColumns = new TreeSet<ColumnMetadata>(columnMetadataComparator);
    for (ColumnMetadata.Raw rawCol : rawCols.values()) {
      DataType dataType;
      if (cassandraVersion.getMajor() >= 3) {
        dataType =
            DataTypeCqlNameParser.parse(
                rawCol.dataType,
                cluster,
                keyspace.getName(),
                keyspace.userTypes,
                keyspace.userTypes,
                false,
                false);
      } else {
        ProtocolVersion protocolVersion =
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        dataType =
            DataTypeClassNameParser.parseOne(rawCol.dataType, protocolVersion, codecRegistry);
      }
      ColumnMetadata col = ColumnMetadata.fromRaw(view, rawCol, dataType);
      switch (rawCol.kind) {
        case PARTITION_KEY:
          partitionKey.set(rawCol.position, col);
          break;
        case CLUSTERING_COLUMN:
          clusteringColumns.set(rawCol.position, col);
          clusteringOrder.set(
              rawCol.position, rawCol.isReversed ? ClusteringOrder.DESC : ClusteringOrder.ASC);
          break;
        default:
          otherColumns.add(col);
          break;
      }
    }
    for (ColumnMetadata c : partitionKey) columns.put(c.getName(), c);
    for (ColumnMetadata c : clusteringColumns) columns.put(c.getName(), c);
    for (ColumnMetadata c : otherColumns) columns.put(c.getName(), c);

    baseTable.add(view);

    return view;
  }

  private static int findCollectionSize(
      Collection<ColumnMetadata.Raw> cols, ColumnMetadata.Raw.Kind kind) {
    int maxId = -1;
    for (ColumnMetadata.Raw col : cols) if (col.kind == kind) maxId = Math.max(maxId, col.position);
    return maxId + 1;
  }

  /**
   * Return this materialized view's base table.
   *
   * @return this materialized view's base table.
   */
  public TableMetadata getBaseTable() {
    return baseTable;
  }

  @Override
  protected String asCQLQuery(boolean formatted) {

    String keyspaceName = Metadata.quoteIfNecessary(keyspace.getName());
    String baseTableName = Metadata.quoteIfNecessary(baseTable.getName());
    String viewName = Metadata.quoteIfNecessary(name);

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE MATERIALIZED VIEW ")
        .append(keyspaceName)
        .append('.')
        .append(viewName)
        .append(" AS");

    // SELECT
    spaceOrNewLine(sb, formatted).append("SELECT ");
    if (includeAllColumns) {
      sb.append("*");
    } else {
      Iterator<ColumnMetadata> it = columns.values().iterator();
      while (it.hasNext()) {
        ColumnMetadata column = it.next();
        sb.append(Metadata.quoteIfNecessary(column.getName()));
        if (it.hasNext()) sb.append(", ");
      }
    }

    // FROM
    spaceOrNewLine(sb, formatted)
        .append("FROM ")
        .append(keyspaceName)
        .append('.')
        .append(baseTableName);

    // WHERE
    // the CQL grammar allows missing WHERE clauses, although C* currently disallows it
    if (whereClause != null && !whereClause.isEmpty()) {
      spaceOrNewLine(sb, formatted).append("WHERE ").append(whereClause);
    }

    // PK
    spaceOrNewLine(sb, formatted).append("PRIMARY KEY (");
    if (partitionKey.size() == 1) {
      sb.append(Metadata.quoteIfNecessary(partitionKey.get(0).getName()));
    } else {
      sb.append('(');
      boolean first = true;
      for (ColumnMetadata cm : partitionKey) {
        if (first) first = false;
        else sb.append(", ");
        sb.append(Metadata.quoteIfNecessary(cm.getName()));
      }
      sb.append(')');
    }
    for (ColumnMetadata cm : clusteringColumns)
      sb.append(", ").append(Metadata.quoteIfNecessary(cm.getName()));
    sb.append(')');

    // append 3 extra spaces if formatted to align WITH.
    spaceOrNewLine(sb, formatted);
    appendOptions(sb, formatted);
    return sb.toString();
  }

  /**
   * Updates the base table for this view and adds it to that table. This is used when a table
   * update is processed and the views need to be carried over.
   */
  void setBaseTable(TableMetadata table) {
    this.baseTable = table;
    table.add(this);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof MaterializedViewMetadata)) return false;

    MaterializedViewMetadata that = (MaterializedViewMetadata) other;
    return MoreObjects.equal(this.name, that.name)
        && MoreObjects.equal(this.id, that.id)
        && MoreObjects.equal(this.partitionKey, that.partitionKey)
        && MoreObjects.equal(this.clusteringColumns, that.clusteringColumns)
        && MoreObjects.equal(this.columns, that.columns)
        && MoreObjects.equal(this.options, that.options)
        && MoreObjects.equal(this.clusteringOrder, that.clusteringOrder)
        && MoreObjects.equal(this.baseTable.getName(), that.baseTable.getName())
        && this.includeAllColumns == that.includeAllColumns;
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(
        name,
        id,
        partitionKey,
        clusteringColumns,
        columns,
        options,
        clusteringOrder,
        baseTable.getName(),
        includeAllColumns);
  }
}
