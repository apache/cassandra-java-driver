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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.DefaultPreparedStatement;
import com.google.common.collect.MapMaker;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestProcessorRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorRegistry.class);

  public static RequestProcessorRegistry defaultCqlProcessors(String logPrefix) {
    ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache =
        new MapMaker().weakValues().makeMap();

    return new RequestProcessorRegistry(
        logPrefix,
        new CqlRequestSyncProcessor(),
        new CqlRequestAsyncProcessor(),
        new CqlPrepareSyncProcessor(preparedStatementsCache),
        new CqlPrepareAsyncProcessor(preparedStatementsCache));
  }

  private final String logPrefix;
  // Effectively immutable: the contents are never modified after construction
  private final RequestProcessor<?, ?>[] processors;

  public RequestProcessorRegistry(String logPrefix, RequestProcessor<?, ?>... processors) {
    this.logPrefix = logPrefix;
    this.processors = processors;
  }

  public <RequestT extends Request, ResultT> RequestProcessor<RequestT, ResultT> processorFor(
      RequestT request, GenericType<ResultT> resultType) {

    for (RequestProcessor<?, ?> processor : processors) {
      if (processor.canProcess(request, resultType)) {
        LOG.trace("[{}] Using {} to process {}", logPrefix, processor, request);
        // The cast is safe provided that the processor implements canProcess correctly
        @SuppressWarnings("unchecked")
        RequestProcessor<RequestT, ResultT> result =
            (RequestProcessor<RequestT, ResultT>) processor;
        return result;
      } else {
        LOG.trace("[{}] {} cannot process {}, trying next", logPrefix, processor, request);
      }
    }
    throw new IllegalArgumentException("No request processor found for " + request);
  }
}
