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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

/**
 * A nexus to send requests to a Cassandra cluster.
 *
 * <p>This is a high-level abstraction capable of handling arbitrary request and result types. For
 * CQL statements, {@link CqlSession} provides convenience methods with more familiar signatures.
 *
 * <p>The driver's request execution logic is pluggable (see {@code RequestProcessor} in the
 * internal API). This is intended for future extensions, for example a reactive API for CQL
 * statements, or graph requests in the Datastax Enterprise driver. Hence the generic {@link
 * #execute(Request, GenericType)} method in this interface, that makes no assumptions about the
 * request or result type.
 */
public interface Session extends AsyncAutoCloseable {

  /**
   * The keyspace that this session is currently connected to.
   *
   * <p>There are two ways that this can be set: during initialization, if the session was created
   * with {@link Cluster#connect(CqlIdentifier)} or {@link Cluster#connectAsync(CqlIdentifier)}; at
   * runtime, if the client issues a request that changes the keyspace (such as a CQL {@code USE}
   * query). Note that this second method is inherently unsafe, since other requests expecting the
   * old keyspace might be executing concurrently. Therefore it is highly discouraged, aside from
   * trivial cases (such as a cqlsh-style program where requests are never concurrent).
   */
  CqlIdentifier getKeyspace();

  /**
   * Executes an arbitrary request.
   *
   * @param resultType the type of the result, which determines the internal request processor
   *     (built-in or custom) that will be used to handle the request.
   * @see Session
   */
  <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType);
}
