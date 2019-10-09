/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;

/**
 * Specialized table or materialized view metadata for DSE.
 *
 * <p>This type exists only for future extensibility; currently, it is identical to {@link
 * RelationMetadata}.
 *
 * <p>Note that all returned {@link ColumnMetadata} can be cast to {@link DseColumnMetadata}.
 */
public interface DseRelationMetadata extends RelationMetadata {}
