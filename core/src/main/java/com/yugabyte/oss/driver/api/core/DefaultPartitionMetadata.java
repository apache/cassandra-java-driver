package com.yugabyte.oss.driver.api.core;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class DefaultPartitionMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitionMetadata.class);

  // Query to load partition metadata for all tables.
  private static final String SELECT_PARTITIONS =
      "SELECT keyspace_name, table_name, start_key, end_key, replica_addresses FROM system.partitions";
  // Assume system queries never need paging
  private static final int INFINITE_PAGE_SIZE = -1;
  private static final Set<String> SYSTEM_KEYSPACES =
      ImmutableSet.of(
          "system", "system_auth", "system_distributed", "system_schema", "system_traces");

  //  protected static DefaultPartitionMetadata build(InternalDriverContext context) {
  //
  //    return new DefaultPartitionMetadata(context);
  //  }

  private final String logPrefix;
  private final ControlConnection controlConnection;
  private final Duration timeout;
  private final CompletableFuture<Void> closeFuture;
  private final boolean partitionMetadataEnabled;
  private final Map<QualifiedTableName, TableSplitMetadata> tableSplits;

  public DefaultPartitionMetadata(
      String logPrefix,
      ControlConnection controlConnection,
      Duration timeout,
      boolean partitionMetadataEnabled,
      Map<UUID, Node> nodes) {

    this.logPrefix = logPrefix;
    this.controlConnection = controlConnection;
    this.timeout = timeout;
    this.closeFuture = new CompletableFuture<>();
    this.partitionMetadataEnabled = partitionMetadataEnabled;
    this.tableSplits = refreshPartitionMap(this.partitionMetadataEnabled, nodes);
  }

  public Map<QualifiedTableName, TableSplitMetadata> getPartitionMap() {
    return tableSplits;
  }

  /**
   * Returns the partition split metadata for a given table.
   *
   * @param keyspaceName the keyspace name
   * @param tableName the table name
   * @return the table split metadata
   */
  public TableSplitMetadata getTableSplitMetadata(String keyspaceName, String tableName) {
    if (tableSplits == null) {
      return null;
    }

    return tableSplits.get(new QualifiedTableName(keyspaceName, tableName));
  }

  private Map<QualifiedTableName, TableSplitMetadata> refreshPartitionMap(
      boolean partitionMetadataEnabled, Map<UUID, Node> nodes) {

    Map<QualifiedTableName, TableSplitMetadata> tableSplits =
        new HashMap<QualifiedTableName, TableSplitMetadata>();

    if (closeFuture.isDone()) {
      CompletableFutures.failedFuture(new IllegalStateException("closed"))
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  Loggers.warnWithException(
                      LOG,
                      "[{}] Unexpected error while refreshing Partition Metadata, "
                          + "keeping previous version",
                      logPrefix,
                      error);
                }
              });
      return null;
    }
    // Refresh partition metadata (table-specific partition splits).
    if (partitionMetadataEnabled) {

      // Prepare the host map to look up host by the inet address.
      Map<InetAddress, Node> hostMap = new HashMap<InetAddress, Node>();
      nodes.forEach(
          (id, node) -> {
            hostMap.put(node.getBroadcastRpcAddress().get().getAddress(), node);
          });

      DriverChannel channel = controlConnection.channel();
      query(channel, SELECT_PARTITIONS)
          .thenApply(result -> createPartitionMap(result, hostMap))
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  Loggers.warnWithException(
                      LOG,
                      "[{}] Unexpected error while refreshing Partition Metadata, "
                          + "keeping previous version",
                      logPrefix,
                      error);
                } else {
                  tableSplits.putAll(result.get());
                }
              });
    }
    return tableSplits;
  }

  private Optional<Map<QualifiedTableName, TableSplitMetadata>> createPartitionMap(
      AdminResult result, Map<InetAddress, Node> hostMap) {

    Map<QualifiedTableName, TableSplitMetadata> tableSplits =
        new HashMap<QualifiedTableName, TableSplitMetadata>();

    for (AdminRow row : result) {

      QualifiedTableName tableId =
          new QualifiedTableName(row.getString("keyspace_name"), row.getString("table_name"));
      TableSplitMetadata tableSplitMetadata = tableSplits.get(tableId);
      if (tableSplitMetadata == null) {
        tableSplitMetadata = new TableSplitMetadata();
        tableSplits.put(tableId, tableSplitMetadata);
      }

      Map<InetAddress, String> replicaAddresses =
          row.getMapOfInetAddressToString("replica_addresses");

      List<Node> hosts = new ArrayList<Node>();
      for (Map.Entry<InetAddress, String> entry : replicaAddresses.entrySet()) {

        Node host = hostMap.get(entry.getKey());
        if (host == null) {
          // Ignore tables in system keyspaces because they are hosted in master not
          // tserver.
          if (!isSystem(tableId.getKeyspaceName())) {
            LOG.debug(
                "Host "
                    + entry.getKey()
                    + " not found in cluster metadata for table "
                    + tableId.toString());
          }
          continue;
        }
        // Put the leader at the beginning and the rest after.
        String role = entry.getValue();
        if (role.equals("LEADER")) {
          hosts.add(0, host);
        } else if (role.equals("FOLLOWER") || role.equals("READ_REPLICA")) {
          hosts.add(host);
        }
      }
      int startKey = getKey(row.getByteBuffer("start_key"));
      int endKey = getKey(row.getByteBuffer("end_key"));
      tableSplitMetadata
          .getPartitionMap()
          .put(startKey, new PartitionMetadata(startKey, endKey, hosts));
    }
    return Optional.ofNullable(tableSplits);
  }

  private CompletionStage<AdminResult> query(
      DriverChannel channel, String queryString, Map<String, Object> parameters) {
    AdminRequestHandler<AdminResult> handler;
    try {
      handler =
          AdminRequestHandler.query(
              channel, queryString, parameters, timeout, INFINITE_PAGE_SIZE, logPrefix);
    } catch (Exception e) {
      return CompletableFutures.failedFuture(e);
    }
    return handler.start();
  }

  private CompletionStage<AdminResult> query(DriverChannel channel, String queryString) {
    return query(channel, queryString, Collections.emptyMap());
  }

  private boolean isSystem(String keyspaceName) {
    return SYSTEM_KEYSPACES.contains(keyspaceName);
  }

  /** Extracts an unsigned 16-bit number from a {@code ByteBuffer}. */
  private static int getKey(ByteBuffer bb) {
    int key = (bb.remaining() == 0) ? 0 : bb.getShort();
    // Flip the negative values back to positive.
    return (key >= 0) ? key : key + 0x10000;
  }
}
