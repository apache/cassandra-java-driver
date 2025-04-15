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
package com.datastax.oss.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestIdGenerator;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import edu.umd.cs.findbugs.annotations.NonNull;

public class UuidRequestIdGenerator implements RequestIdGenerator {
  public UuidRequestIdGenerator(DriverContext context) {}

  /** Generates a random v4 UUID. */
  @Override
  public String getSessionRequestId() {
    return Uuids.random().toString();
  }

  /**
   * {session-request-id}-{random-uuid} All node requests for a session request will have the same
   * session request id
   */
  @Override
  public String getNodeRequestId(@NonNull Request statement, @NonNull String parentId) {
    return parentId + "-" + Uuids.random();
  }
}
