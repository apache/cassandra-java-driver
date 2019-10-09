/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema.parsing;

import com.datastax.dse.driver.api.core.metadata.schema.DseAggregateMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseAggregateMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.AggregateParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DseAggregateParser {

  private final AggregateParser aggregateParser;
  private final InternalDriverContext context;

  public DseAggregateParser(DataTypeParser dataTypeParser, InternalDriverContext context) {
    this.aggregateParser = new AggregateParser(dataTypeParser, context);
    this.context = context;
  }

  public DseAggregateMetadata parseAggregate(
      AdminRow row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    AggregateMetadata aggregate = aggregateParser.parseAggregate(row, keyspaceId, userDefinedTypes);
    // parse the DSE extended columns
    final boolean deterministic =
        row.contains("deterministic") ? row.getBoolean("deterministic") : false;

    return new DefaultDseAggregateMetadata(
        aggregate.getKeyspace(),
        aggregate.getSignature(),
        aggregate.getFinalFuncSignature().orElse(null),
        aggregate.getInitCond().orElse(null),
        aggregate.getReturnType(),
        aggregate.getStateFuncSignature(),
        aggregate.getStateType(),
        context.getCodecRegistry().codecFor(aggregate.getStateType()),
        deterministic);
  }
}
