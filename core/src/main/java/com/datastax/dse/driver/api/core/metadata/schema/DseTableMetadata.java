/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Specialized table metadata for DSE.
 *
 * <p>This type exists only for future extensibility; currently, it is identical to {@link
 * TableMetadata}.
 *
 * <p>Note that all returned {@link ColumnMetadata} can be cast to {@link DseColumnMetadata}, and
 * all {@link IndexMetadata} to {@link DseIndexMetadata}.
 */
public interface DseTableMetadata extends DseRelationMetadata, TableMetadata {}
