/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseIndexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultIndexMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseIndexMetadata extends DefaultIndexMetadata implements DseIndexMetadata {

  public DefaultDseIndexMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      @NonNull CqlIdentifier name,
      @NonNull IndexKind kind,
      @NonNull String target,
      @NonNull Map<String, String> options) {
    super(keyspace, table, name, kind, target, options);
  }
}
