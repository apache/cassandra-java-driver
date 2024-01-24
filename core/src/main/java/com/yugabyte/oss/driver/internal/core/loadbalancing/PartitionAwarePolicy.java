// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.oss.driver.internal.core.loadbalancing;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.yugabyte.oss.driver.api.core.DefaultPartitionMetadata;
import com.yugabyte.oss.driver.api.core.TableSplitMetadata;
import com.yugabyte.oss.driver.api.core.utils.Jenkins;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class PartitionAwarePolicy extends YugabyteDefaultLoadBalancingPolicy
    implements RequestTracker {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionAwarePolicy.class);

  public PartitionAwarePolicy(@NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
  }

  @Override
  public void init(Map<UUID, Node> nodes, DistanceReporter distanceReporter) {
    super.init(nodes, distanceReporter);
  }

  @Override
  public Queue<Node> newQueryPlan(Request request, Session session) {

    Iterator<Node> partitionAwareNodeIterator = null;
    if (request instanceof BoundStatement) {
      partitionAwareNodeIterator = getQueryPlan(session, (BoundStatement) request);
    } else if (request instanceof BatchStatement) {
      partitionAwareNodeIterator = getQueryPlan(session, (BatchStatement) request);
    }

    LinkedHashSet<Node> partitionAwareNodes = null;
    if (partitionAwareNodeIterator != null) {
      partitionAwareNodes = new LinkedHashSet<>();
      while (partitionAwareNodeIterator.hasNext()) {
        partitionAwareNodes.add(partitionAwareNodeIterator.next());
      }
      LOG.debug("newQueryPlan: Number of Nodes = " + partitionAwareNodes.size());
    }

    // It so happens that the partition aware nodes could be non-empty, but the state of the nodes
    // could be down.
    // In such cases fallback to the inherited load-balancing logic
    return !(partitionAwareNodes == null || partitionAwareNodes.isEmpty())
        ? new SimpleQueryPlan(partitionAwareNodes.toArray())
        : super.newQueryPlan(request, session);
  }

  /**
   * Gets the query plan for a {@code BoundStatement}.
   *
   * @param session db session
   * @param statement the statement
   * @return the query plan, or null when no plan can be determined
   */
  private Iterator<Node> getQueryPlan(Session session, BoundStatement statement) {
    PreparedStatement pstmt = statement.getPreparedStatement();
    String query = pstmt.getQuery();
    ColumnDefinitions variables = pstmt.getVariableDefinitions();
    // Look up the hosts for the partition key. Skip statements that do not have
    // bind variables.
    if (variables.size() == 0) return null;
    int key = getKey(statement);
    if (key < 0) return null;
    String queryKeySpace = variables.get(0).getKeyspace().asInternal();
    String queryTable = variables.get(0).getTable().asInternal();

    LOG.debug("getQueryPlan: keyspace = " + queryKeySpace + ", query = " + query);

    Optional<DefaultPartitionMetadata> partitionMetadata =
        session.getMetadata().getDefaultPartitionMetadata();
    if (!partitionMetadata.isPresent()) {
      return null;
    }

    TableSplitMetadata tableSplitMetadata =
        partitionMetadata.get().getTableSplitMetadata(queryKeySpace, queryTable);
    if (tableSplitMetadata == null) {
      return null;
    }

    // Get all the applicable nodes for LoadBalancing from the base class
    Iterator<Node> nodesFromBasePolicy =
        super.newQueryPlan((Request) statement, session).iterator();

    // This needs to manipulate the local copy of the hosts instead of the actual reference
    return new UpHostIterator(
        statement, new ArrayList(tableSplitMetadata.getHosts(key)), nodesFromBasePolicy);
  }

  /**
   * Gets the query plan for a {@code BatchStatement}.
   *
   * @param session db session
   * @param batch the batch statement
   * @return the query plan, or null when no plan can be determined
   */
  private Iterator<Node> getQueryPlan(Session session, BatchStatement batch) {

    Iterator<BatchableStatement<?>> batchIterator = batch.iterator();
    while (batchIterator.hasNext()) {
      BatchableStatement<?> nextStatement = batchIterator.next();
      if (nextStatement instanceof BoundStatement) {
        Iterator<Node> plan = getQueryPlan(session, (BoundStatement) nextStatement);
        if (plan != null) return plan;
      }
    }
    return null;
  }

  /**
   * An iterator that returns hosts to executing a given statement, selecting only the hosts that
   * are up and local from the given hosts that host the statement's partition key, and then the
   * ones from the child policy.
   */
  private static class UpHostIterator implements Iterator<Node> {

    private final BoundStatement statement;
    private final Iterator<Node> iterator;
    private final Iterator<Node> childIterator;
    private final List<Node> hosts;
    private Node nextHost;

    /**
     * Creates a new {@code UpHostIterator}.
     *
     * @param statement the statement
     * @param hosts the hosts that host the statement's partition key
     */
    public UpHostIterator(
        BoundStatement statement, List<Node> hosts, Iterator<Node> nodesFromBasePolicy) {
      this.statement = statement;
      this.hosts = hosts;
      this.iterator = hosts.iterator();
      this.childIterator = nodesFromBasePolicy;

      // When the CQL consistency level is set to YB consistent prefix (Cassandra
      // ONE),
      // the reads would end up going only to the leader if the list of hosts are not
      // shuffled.
      if (getConsistencyLevel() == ConsistencyLevel.YB_CONSISTENT_PREFIX) {
        // this is to be performed in the local copy
        Collections.shuffle(hosts);
      }
    }

    private ConsistencyLevel getConsistencyLevel() {
      return statement.getConsistencyLevel() != null
          ? statement.getConsistencyLevel()
          : ConsistencyLevel.YB_STRONG;
    }

    @Override
    public boolean hasNext() {

      while (iterator.hasNext()) {
        nextHost = iterator.next();
        // If the host is up, use it if it is local, or the statement requires strong
        // consistency.
        // In the latter case, we want to use the first available host since the leader
        // is in the
        // head of the host list.

        if (nextHost.getState() == NodeState.UP
            && (nextHost.getDistance() == NodeDistance.LOCAL
                || getConsistencyLevel().isYBStrong())) {
          return true;
        }
      }

      if (childIterator != null) {
        while (childIterator.hasNext()) {
          nextHost = childIterator.next();
          // Skip host if it is a local host that we have already returned earlier.
          if (!hosts.contains(nextHost)
              || !(nextHost.getDistance() == NodeDistance.LOCAL
                  || statement.getConsistencyLevel() == ConsistencyLevel.YB_STRONG)) return true;
        }
      }

      return false;
    }

    @Override
    public Node next() {
      return nextHost;
    }
  }

  /**
   * Returns the hash key for the given bytes. The hash key is an unsigned 16-bit number.
   *
   * @param bytes the bytes to calculate the hash key
   * @return the hash key
   */
  private static int getKey(byte bytes[]) {
    final long SEED = 97;
    long h = Jenkins.hash64(bytes, SEED);
    long h1 = h >>> 48;
    long h2 = 3 * (h >>> 32);
    long h3 = 5 * (h >>> 16);
    long h4 = 7 * (h & 0xffff);
    return (int) ((h1 ^ h2 ^ h3 ^ h4) & 0xffff);
  }

  /**
   * Internally we use a unsigned 16-bit hash CQL uses signed 64-bit. This method converts from the
   * user-visible (e.g. via 'token') CQL hash to our internal representation used for partitioning.
   *
   * @param cql_hash the CQL hash key
   * @return the corresponding internal YB hash key
   */
  public static int CqlToYBHashCode(long cql_hash) {
    int hash_code = (int) (cql_hash >> 48);
    hash_code ^= 0x8000; // flip first bit so that negative values are smaller than positives.
    return hash_code;
  }

  /**
   * Internally we use a unsigned 16-bit hash CQL uses signed 64-bit. This method converts from our
   * internal representation used for partitioning to the user-visible (e.g. via 'token') CQL hash.
   *
   * @param hash the internal YB hash key
   * @return a corresponding CQL hash key
   */
  public static long YBToCqlHashCode(int hash) {
    long cql_hash = hash ^ 0x8000; // undo the flipped bit
    cql_hash = cql_hash << 48;
    return cql_hash;
  }

  /**
   * Returns the hash key for the given bound statement. The hash key can be determined only for
   * DMLs and when the partition key is specified in the bind variables.
   *
   * @param stmt the statement to calculate the hash key for
   * @return the hash key for the statement, or -1 when hash key cannot be determined
   */
  public static int getKey(BoundStatement stmt) {
    PreparedStatement pstmt = stmt.getPreparedStatement();
    List<Integer> hashIndexes = pstmt.getPartitionKeyIndices();

    // Return if no hash key indexes are found, such as when the hash column values
    // are literal
    // constants.
    if (hashIndexes == null || hashIndexes.isEmpty()) {
      return -1;
    }

    // Compute the hash key bytes, i.e. <h1><h2>...<h...>.
    try {
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      WritableByteChannel channel = Channels.newChannel(bs);
      ColumnDefinitions variables = pstmt.getVariableDefinitions();
      for (int i = 0; i < hashIndexes.size(); i++) {
        int index = hashIndexes.get(i);
        DataType type = variables.get(index).getType();
        ByteBuffer value = stmt.getBytesUnsafe(index).duplicate();
        AppendValueToChannel(type, value, channel);
      }
      channel.close();

      return getKey(bs.toByteArray());
    } catch (IOException e) {
      // IOException should not happen at all given we are writing to the in-memory
      // buffer only. So
      // if it does happen, we just want to log the error but fallback to the default
      // set of hosts.
      LOG.error("hash key encoding failed", e);
      return -1;
    }
  }

  private static void AppendValueToChannel(
      DataType type, ByteBuffer value, WritableByteChannel channel) throws java.io.IOException {
    int typeCode = type.getProtocolCode();

    switch (typeCode) {
      case ProtocolConstants.DataType.BOOLEAN:
      case ProtocolConstants.DataType.TINYINT:
      case ProtocolConstants.DataType.SMALLINT:
      case ProtocolConstants.DataType.INT:
      case ProtocolConstants.DataType.BIGINT:
      case ProtocolConstants.DataType.ASCII:
        // case ProtocolConstants.DataType.TEXT:
      case ProtocolConstants.DataType.JSONB:
      case ProtocolConstants.DataType.VARCHAR:
      case ProtocolConstants.DataType.BLOB:
      case ProtocolConstants.DataType.INET:
      case ProtocolConstants.DataType.UUID:
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.DATE:
        /*
         * Decimal Type is supported column type for partitioned keys, However One-hop
         * query fetch for Decimal Type is not supported as hash-code cannot be computed
         * for Decimal types.
         */
      case ProtocolConstants.DataType.DECIMAL:
      case ProtocolConstants.DataType.VARINT:
      case ProtocolConstants.DataType.TIME:
        channel.write(value);
        break;
      case ProtocolConstants.DataType.FLOAT:
        {
          float floatValue = value.getFloat(0);
          value.rewind();
          if (Float.isNaN(floatValue)) {
            // Normalize NaN byte representation.
            value = ByteBuffer.allocate(4);
            value.putInt(0xff << 23 | 0x1 << 22);
            value.flip();
          }
          channel.write(value);
          break;
        }
      case ProtocolConstants.DataType.DOUBLE:
        {
          double doubleValue = value.getDouble(0);
          value.rewind();
          if (Double.isNaN(doubleValue)) {
            // Normalize NaN byte representation.
            value = ByteBuffer.allocate(8);
            value.putLong((long) 0x7ff << 52 | (long) 0x1 << 51);
            value.flip();
          }
          channel.write(value);
          break;
        }
      case ProtocolConstants.DataType.TIMESTAMP:
        {
          // Multiply the timestamp's int64 value by 1000 to adjust the precision.
          ByteBuffer bb = ByteBuffer.allocate(8);
          bb.putLong(value.getLong() * 1000);
          bb.flip();
          value = bb;
          channel.write(value);
          break;
        }
      case ProtocolConstants.DataType.LIST:
        {
          ListType listType = (ListType) type;
          DataType dataTypeOfListValue = listType.getElementType();
          int length = value.getInt();
          for (int j = 0; j < length; j++) {
            // Appending each element.
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(dataTypeOfListValue, buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case ProtocolConstants.DataType.SET:
        {
          SetType setType = (SetType) type;
          DataType dataTypeOfSetValue = setType.getElementType();
          int length = value.getInt();
          for (int j = 0; j < length; j++) {
            // Appending each element.
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(dataTypeOfSetValue, buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case ProtocolConstants.DataType.MAP:
        {
          MapType mapType = (MapType) type;
          DataType dataTypeOfMapKey = mapType.getKeyType();
          DataType dataTypeOfMapValue = mapType.getValueType();
          int length = value.getInt();
          for (int j = 0; j < length; j++) {
            // Appending the key.
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(dataTypeOfMapKey, buf, channel);
            value.position(value.position() + size);
            // Appending the value.
            size = value.getInt();
            buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(dataTypeOfMapValue, buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case ProtocolConstants.DataType.UDT:
        {
          UserDefinedType udt = (UserDefinedType) type;

          for (DataType field : udt.getFieldTypes()) {
            if (!value.hasRemaining()) {
              // UDT serialization may omit values of last few fields if they are null.
              break;
            }
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(field, buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case ProtocolConstants.DataType.COUNTER:
      case ProtocolConstants.DataType.CUSTOM:
      case ProtocolConstants.DataType.TUPLE:
        throw new UnsupportedOperationException(
            "Datatype with Hex Code: " + typeCode + " not supported in a partition key column");
    }
  }
}
