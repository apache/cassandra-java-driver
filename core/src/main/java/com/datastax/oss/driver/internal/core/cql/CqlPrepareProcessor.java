/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestHandler;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.google.common.collect.MapMaker;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the preparation of CQL queries.
 *
 * <p>This class is stateful, you can't reuse the same instance across multiple {@link Cluster}
 * instances.
 */
public class CqlPrepareProcessor
    implements RequestProcessor<PreparedStatement, CompletionStage<PreparedStatement>> {

  private static final Logger LOG = LoggerFactory.getLogger(CqlPrepareProcessor.class);

  private final ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatements =
      new MapMaker().weakValues().makeMap();

  @Override
  public boolean canProcess(Request<?, ?> request) {
    return request instanceof PrepareRequest;
  }

  @Override
  public RequestHandler<PreparedStatement, CompletionStage<PreparedStatement>> newHandler(
      Request<PreparedStatement, CompletionStage<PreparedStatement>> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new CqlPrepareHandler(
        (PrepareRequest) request, this, session, context, sessionLogPrefix);
  }

  DefaultPreparedStatement cache(DefaultPreparedStatement preparedStatement) {
    DefaultPreparedStatement previous =
        preparedStatements.putIfAbsent(preparedStatement.getId(), preparedStatement);
    if (previous != null) {
      LOG.warn(
          "Re-preparing already prepared query. "
              + "This is generally an anti-pattern and will likely affect performance. "
              + "Consider preparing the statement only once. Query='{}'",
          preparedStatement.getQuery());

      // The one object in the cache will get GCed once it's not referenced by the client anymore
      // since we use a weak reference. So we need to make sure that the instance we do return to
      // the user is the one that is in the cache.
      return previous;
    } else {
      return preparedStatement;
    }
  }
}
