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
package com.datastax.oss.driver.api.core.tracker;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface responsible for generating request IDs.
 *
 * <p>Note that all request IDs have a parent/child relationship. A "session request ID" can loosely
 * be thought of as encompassing a sequence of a request + any attendant retries, speculative
 * executions etc. It's scope is identical to that of a {@link
 * com.datastax.oss.driver.internal.core.cql.CqlRequestHandler}. A "node request ID" represents a
 * single request within this larger scope. Note that a request corresponding to a request ID may be
 * retried; in that case the retry count will be appended to the corresponding identifier in the
 * logs.
 */
public interface RequestIdGenerator {

  String DEFAULT_PAYLOAD_KEY = "request-id";

  /**
   * Generates a unique identifier for the session request. This will be the identifier for the
   * entire `session.execute()` call. This identifier will be added to logs, and propagated to
   * request trackers.
   *
   * @return a unique identifier for the session request
   */
  String getSessionRequestId();

  /**
   * Generates a unique identifier for the node request. This will be the identifier for the CQL
   * request against a particular node. There can be one or more node requests for a single session
   * request, due to retries or speculative executions. This identifier will be added to logs, and
   * propagated to request trackers.
   *
   * @param statement the statement to be executed
   * @param parentId the session request identifier
   * @return a unique identifier for the node request
   */
  String getNodeRequestId(@NonNull Request statement, @NonNull String parentId);

  default String getCustomPayloadKey() {
    return DEFAULT_PAYLOAD_KEY;
  }

  default Statement<?> getDecoratedStatement(
      @NonNull Statement<?> statement, @NonNull String requestId) {

    Map<String, ByteBuffer> existing = new HashMap<>(statement.getCustomPayload());
    String key = getCustomPayloadKey();

    // Add or overwrite
    existing.put(key, ByteBuffer.wrap(requestId.getBytes(StandardCharsets.UTF_8)));

    // Allowing null key/values
    // Wrap a map inside to be immutable without instanciating a new map
    Map<String, ByteBuffer> unmodifiableMap = Collections.unmodifiableMap(existing);

    return statement.setCustomPayload(unmodifiableMap);
  }
}
