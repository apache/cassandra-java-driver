package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.annotations.Beta;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/** Holds all currently known tablet metadata. */
@Beta
public interface TabletMap {
  /**
   * Returns mapping from tables to the sets of their tablets.
   *
   * @return the Map keyed by (keyspace,table) pairs with Set of tablets as value type.
   */
  public ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> getMapping();

  /**
   * Adds a single tablet to the map. Handles removal of overlapping tablets.
   *
   * @param keyspace target keyspace
   * @param table target table
   * @param tablet tablet instance to add
   */
  public void addTablet(CqlIdentifier keyspace, CqlIdentifier table, Tablet tablet);

  /**
   * Returns {@link Tablet} instance
   *
   * @param keyspace tablet's keyspace
   * @param table tablet's table
   * @param token target token
   * @return {@link Tablet} responsible for provided token or {@code null} if no such tablet is
   *     present.
   */
  public Tablet getTablet(CqlIdentifier keyspace, CqlIdentifier table, long token);
}
