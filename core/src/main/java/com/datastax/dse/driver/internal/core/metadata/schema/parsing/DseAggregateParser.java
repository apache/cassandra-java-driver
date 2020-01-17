/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    final Boolean deterministic =
        row.contains("deterministic") ? row.getBoolean("deterministic") : null;

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
