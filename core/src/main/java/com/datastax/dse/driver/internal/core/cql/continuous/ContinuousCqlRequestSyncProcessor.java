/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ContinuousCqlRequestSyncProcessor
    implements RequestProcessor<Statement<?>, ContinuousResultSet> {

  public static final GenericType<ContinuousResultSet> CONTINUOUS_RESULT_SYNC =
      GenericType.of(ContinuousResultSet.class);

  private final ContinuousCqlRequestAsyncProcessor asyncProcessor;

  public ContinuousCqlRequestSyncProcessor(ContinuousCqlRequestAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(CONTINUOUS_RESULT_SYNC);
  }

  @Override
  public ContinuousResultSet process(
      Statement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    BlockingOperation.checkNotDriverThread();
    ContinuousAsyncResultSet firstPage =
        CompletableFutures.getUninterruptibly(
            asyncProcessor.process(request, session, context, sessionLogPrefix));
    return new DefaultContinuousResultSet(firstPage);
  }

  @Override
  public ContinuousResultSet newFailure(RuntimeException error) {
    throw error;
  }
}
