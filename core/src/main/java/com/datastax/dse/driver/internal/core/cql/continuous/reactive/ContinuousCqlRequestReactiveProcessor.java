/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous.reactive;

import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveResultSet;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.cql.reactive.FailedReactiveResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ContinuousCqlRequestReactiveProcessor
    implements RequestProcessor<Statement<?>, ContinuousReactiveResultSet> {

  public static final GenericType<ContinuousReactiveResultSet> CONTINUOUS_REACTIVE_RESULT_SET =
      GenericType.of(ContinuousReactiveResultSet.class);

  private final ContinuousCqlRequestAsyncProcessor asyncProcessor;

  public ContinuousCqlRequestReactiveProcessor(ContinuousCqlRequestAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(CONTINUOUS_REACTIVE_RESULT_SET);
  }

  @Override
  public ContinuousReactiveResultSet process(
      Statement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new DefaultContinuousReactiveResultSet(
        () -> asyncProcessor.process(request, session, context, sessionLogPrefix));
  }

  @Override
  public ContinuousReactiveResultSet newFailure(RuntimeException error) {
    return new FailedReactiveResultSet(error);
  }
}
