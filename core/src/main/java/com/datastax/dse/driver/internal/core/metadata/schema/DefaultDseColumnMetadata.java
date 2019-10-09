/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseColumnMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseColumnMetadata extends DefaultColumnMetadata implements DseColumnMetadata {

  public DefaultDseColumnMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier parent,
      @NonNull CqlIdentifier name,
      @NonNull DataType dataType,
      boolean isStatic) {
    super(keyspace, parent, name, dataType, isStatic);
  }
}
