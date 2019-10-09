/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DseSchemaParserFactory implements SchemaParserFactory {

  private final InternalDriverContext context;

  public DseSchemaParserFactory(InternalDriverContext context) {
    this.context = context;
  }

  @Override
  public SchemaParser newInstance(SchemaRows rows) {
    return new DseSchemaParser(rows, context);
  }
}
