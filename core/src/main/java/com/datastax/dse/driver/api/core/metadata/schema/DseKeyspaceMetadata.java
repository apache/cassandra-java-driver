/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Specialized keyspace metadata for DSE.
 *
 * <p>This type exists only for future extensibility; currently, it is identical to {@link
 * KeyspaceMetadata}.
 *
 * <p>Note that all returned elements can be cast to their DSE counterpart, for example {@link
 * TableMetadata} to {@link DseTableMetadata}.
 */
public interface DseKeyspaceMetadata extends KeyspaceMetadata {}
