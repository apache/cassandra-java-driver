/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;

/**
 * Handles a type of request in the driver.
 *
 * <p>By default, the driver supports CQL {@link Statement queries} and {@link PrepareRequest
 * preparation requests}. New processors can be plugged in to handle new types of requests.
 *
 * @param <RequestT> the type of request accepted.
 * @param <ResultT> the type of result when a request is processed.
 */
public interface RequestProcessor<RequestT extends Request, ResultT> {

  /**
   * Whether the processor can produce the given result from the given request.
   *
   * <p>Processors will be tried in the order they were registered. The first processor for which
   * this method returns true will be used.
   */
  boolean canProcess(Request request, GenericType<?> resultType);

  /** Processes the given request, producing a result. */
  ResultT process(
      RequestT request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix);

  /** Builds a failed result to directly report the given error. */
  ResultT newFailure(RuntimeException error);
}
