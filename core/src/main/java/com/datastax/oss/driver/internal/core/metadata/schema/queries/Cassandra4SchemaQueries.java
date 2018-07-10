package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
class Cassandra4SchemaQueries extends Cassandra3SchemaQueries {
  Cassandra4SchemaQueries(
      DriverChannel channel,
      CompletableFuture<Metadata> refreshFuture,
      DriverConfigProfile config,
      String logPrefix) {
    super(channel, refreshFuture, config, logPrefix);
  }

  @Override
  protected Optional<String> selectVirtualKeyspaces() {
    return Optional.of("SELECT * FROM system_virtual_schema.keyspaces");
  }

  @Override
  protected Optional<String> selectVirtualTables() {
    return Optional.of("SELECT * FROM system_virtual_schema.tables");
  }

  @Override
  protected Optional<String> selectVirtualColumns() {
    return Optional.of("SELECT * FROM system_virtual_schema.columns");
  }
}
