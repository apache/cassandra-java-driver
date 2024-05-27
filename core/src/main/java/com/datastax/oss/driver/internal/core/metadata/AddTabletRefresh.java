package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;

/** Updates tablet metadata by adding provided Tablet to the TabletMap. */
public class AddTabletRefresh implements MetadataRefresh {

  final CqlIdentifier keyspace;
  final CqlIdentifier table;
  final Tablet tablet;

  public AddTabletRefresh(CqlIdentifier keyspace, CqlIdentifier table, Tablet tablet) {
    this.keyspace = keyspace;
    this.table = table;
    this.tablet = tablet;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {
    oldMetadata.tabletMap.addTablet(keyspace, table, tablet);
    return new Result(oldMetadata);
  }
}
