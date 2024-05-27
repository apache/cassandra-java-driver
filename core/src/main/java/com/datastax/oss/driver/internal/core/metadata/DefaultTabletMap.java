package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.KeyspaceTableNamePair;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.shaded.guava.common.annotations.Beta;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds currently known tablet mappings. Updated lazily through received custom payloads described
 * in Scylla's CQL protocol extensions (tablets-routing-v1).
 *
 * <p>Thread-safety notes: This class uses ConcurrentMap and ConcurrentSkipListSet underneath. It is
 * safe to have multiple threads accessing it. However, multiple modifications i.e. multiple calls
 * of {@link DefaultTabletMap#addTablet(CqlIdentifier, CqlIdentifier, Tablet) will race with each
 * other. This may result in unexpected state of this structure when used in a vacuum. For example
 * it may end up containing overlapping tablet ranges.</p> <p>In actual use by the driver {@link
 * MetadataManager} solves this by running modifications sequentially. It schedules them on {@link
 * MetadataManager#adminExecutor}}'s thread.
 */
@Beta
public class DefaultTabletMap implements TabletMap {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTabletMap.class);

  @NonNull
  private final ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> mapping;

  private DefaultTabletMap(
      @NonNull ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> mapping) {
    this.mapping = mapping;
  }

  public static DefaultTabletMap emptyMap() {
    return new DefaultTabletMap(new ConcurrentHashMap<>());
  }

  @Override
  @NonNull
  public ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> getMapping() {
    return mapping;
  }

  @Override
  public Tablet getTablet(CqlIdentifier keyspace, CqlIdentifier table, long token) {
    KeyspaceTableNamePair key = new KeyspaceTableNamePair(keyspace, table);
    NavigableSet<Tablet> set = mapping.get(key);
    if (set == null) {
      LOG.trace(
          "There is no tablets for {}.{} in current mapping. Returning null.", keyspace, table);
      return null;
    }
    Tablet result = mapping.get(key).ceiling(DefaultTablet.malformedTablet(token));
    if (result == null || result.getFirstToken() >= token) {
      LOG.trace(
          "Could not find tablet for {}.{} that owns token {}. Returning null.",
          keyspace,
          table,
          token);
      return null;
    }
    return result;
  }

  @Override
  public void addTablet(CqlIdentifier keyspace, CqlIdentifier table, Tablet tablet) {
    LOG.trace("Adding tablet for {}.{} with contents {}", keyspace, table, tablet);
    KeyspaceTableNamePair ktPair = new KeyspaceTableNamePair(keyspace, table);

    // Get existing tablets for given table
    NavigableSet<Tablet> existingTablets =
        mapping.computeIfAbsent(ktPair, k -> new ConcurrentSkipListSet<>());
    // Single tablet token range is represented by (firstToken, lastToken] interval
    // We need to do two sweeps: remove overlapping tablets by looking at lastToken of existing
    // tablets
    // and then by looking at firstToken of existing tablets. Currently, the tablets are sorted
    // according
    // to their lastTokens.

    // First sweep: remove all tablets whose lastToken is inside this interval
    Iterator<Tablet> it = existingTablets.headSet(tablet, true).descendingIterator();
    while (it.hasNext()) {
      Tablet nextTablet = it.next();
      if (nextTablet.getLastToken() <= tablet.getFirstToken()) {
        break;
      }
      it.remove();
    }

    // Second sweep: remove all tablets that have their lastToken greater than that of
    // the tablet that is being added AND their firstToken is smaller than lastToken of new
    // addition.
    // After the first sweep, this theoretically should remove at most 1 tablet.
    it = existingTablets.tailSet(tablet, true).iterator();
    while (it.hasNext()) {
      Tablet nextTablet = it.next();
      if (nextTablet.getFirstToken() >= tablet.getLastToken()) {
        break;
      }
      it.remove();
    }

    // Add new (now) non-overlapping tablet
    existingTablets.add(tablet);
  }

  /**
   * Represents a single tablet created from tablets-routing-v1 custom payload. Its {@code
   * compareTo} implementation intentionally relies solely on {@code lastToken} in order to allow
   * for quick lookup on sorted Collections based just on the token value. Its token range is
   * (firstToken, lastToken], meaning firstToken is not included.
   */
  public static class DefaultTablet implements Tablet {
    private final long firstToken;
    private final long lastToken;
    @NonNull private final Set<Node> replicaNodes;
    @NonNull private final Map<Node, Integer> replicaShards;

    @VisibleForTesting
    DefaultTablet(
        long firstToken,
        long lastToken,
        @NonNull Set<Node> replicaNodes,
        @NonNull Map<Node, Integer> replicaShards) {
      this.firstToken = firstToken;
      this.lastToken = lastToken;
      this.replicaNodes = replicaNodes;
      this.replicaShards = replicaShards;
    }

    /**
     * Creates a new instance of DefaultTablet based on provided decoded payload.
     *
     * @param tupleValue Decoded tablets-routing-v1 payload
     * @param nodes Mapping of UUIDs to Node instances.
     * @return the new DefaultTablet
     */
    public static DefaultTablet parseTabletPayloadV1(TupleValue tupleValue, Map<UUID, Node> nodes) {

      long firstToken = tupleValue.getLong(0);
      long lastToken = tupleValue.getLong(1);

      Set<Node> replicaNodes = new HashSet<>();
      Map<Node, Integer> replicaShards = new HashMap<>();
      List<TupleValue> list = tupleValue.getList(2, TupleValue.class);
      assert list != null;
      for (TupleValue tuple : list) {
        Node node = nodes.get(tuple.getUuid(0));
        if (node != null) {
          int shard = tuple.getInt(1);
          replicaNodes.add(node);
          replicaShards.put(node, shard);
        }
      }

      return new DefaultTablet(firstToken, lastToken, replicaNodes, replicaShards);
    }

    /**
     * Creates a {@link DefaultTablet} instance with given {@code lastToken}, identical {@code
     * firstToken} and unspecified other fields. Used for lookup of sorted collections of proper
     * {@link DefaultTablet}.
     *
     * @param lastToken
     * @return New {@link DefaultTablet} object
     */
    public static DefaultTablet malformedTablet(long lastToken) {
      return new DefaultTablet(
          lastToken, lastToken, Collections.emptySet(), Collections.emptyMap());
    }

    @Override
    public long getFirstToken() {
      return firstToken;
    }

    @Override
    public long getLastToken() {
      return lastToken;
    }

    @Override
    public Set<Node> getReplicaNodes() {
      return replicaNodes;
    }

    @Override
    public int getShardForNode(Node n) {
      return replicaShards.getOrDefault(n, -1);
    }

    @Override
    public String toString() {
      return "DefaultTablet{"
          + "firstToken="
          + firstToken
          + ", lastToken="
          + lastToken
          + ", replicaNodes="
          + replicaNodes
          + ", replicaShards="
          + replicaShards
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DefaultTablet)) return false;
      DefaultTablet that = (DefaultTablet) o;
      return firstToken == that.firstToken
          && lastToken == that.lastToken
          && replicaNodes.equals(that.replicaNodes)
          && replicaShards.equals(that.replicaShards);
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstToken, lastToken, replicaNodes, replicaShards);
    }

    @Override
    public int compareTo(Tablet tablet) {
      return Long.compare(this.lastToken, tablet.getLastToken());
    }
  }
}
