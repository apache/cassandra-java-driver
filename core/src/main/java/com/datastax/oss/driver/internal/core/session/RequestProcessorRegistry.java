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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.cql.CqlRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestProcessorRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorRegistry.class);

  public static final RequestProcessorRegistry DEFAULT =
      new RequestProcessorRegistry(
          new CqlRequestProcessor()
          // TODO add CqlPrepareProcessor
          );

  // Effectively immutable: the contents are never modified after construction
  private final RequestProcessor<?, ?>[] processors;

  public RequestProcessorRegistry(RequestProcessor<?, ?>... processors) {
    this.processors = processors;
  }

  public <SyncResultT, AsyncResultT> RequestProcessor<SyncResultT, AsyncResultT> processorFor(
      Request<SyncResultT, AsyncResultT> request) {

    for (RequestProcessor<?, ?> processor : processors) {
      if (processor.canProcess(request)) {
        LOG.trace("Using {} to process {}", processor, request);
        // The cast is safe provided that the processor implements canProcess correctly
        @SuppressWarnings("unchecked")
        RequestProcessor<SyncResultT, AsyncResultT> result =
            (RequestProcessor<SyncResultT, AsyncResultT>) processor;
        return result;
      } else {
        LOG.trace("{} cannot process {}, trying next", processor, request);
      }
    }
    throw new IllegalArgumentException("No request processor found for " + request);
  }
}
