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

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;

/**
 * Handles a type of request in the driver.
 *
 * <p>By default, the driver supports CQL {@link Statement queries} and {@link PrepareRequest
 * preparation requests}. New processors can be plugged in to handle new types of requests.
 *
 * @param <SyncResultT> the type of result when a request is executed synchronously.
 * @param <AsyncResultT> the type of result when a request is executed asynchronously.
 */
public interface RequestProcessor<SyncResultT, AsyncResultT> {

  /**
   * Whether the processor can handle a given request.
   *
   * <p>Processors will be tried in the order they were registered. The first processor for which
   * this method returns true will be used.
   */
  boolean canProcess(Request<?, ?> request);

  /** Builds a new handler to process a given request. */
  RequestHandler<SyncResultT, AsyncResultT> newHandler(
      Request<SyncResultT, AsyncResultT> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix);
}
