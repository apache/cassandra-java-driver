/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CqlRequestReactiveProcessor
    implements RequestProcessor<Statement<?>, ReactiveResultSet> {

  public static final GenericType<ReactiveResultSet> REACTIVE_RESULT_SET =
      GenericType.of(ReactiveResultSet.class);

  private final CqlRequestAsyncProcessor asyncProcessor;

  public CqlRequestReactiveProcessor(CqlRequestAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(REACTIVE_RESULT_SET);
  }

  @Override
  public ReactiveResultSet process(
      Statement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new DefaultReactiveResultSet(
        () -> asyncProcessor.process(request, session, context, sessionLogPrefix));
  }

  @Override
  public ReactiveResultSet newFailure(RuntimeException error) {
    return new FailedReactiveResultSet(error);
  }
}
