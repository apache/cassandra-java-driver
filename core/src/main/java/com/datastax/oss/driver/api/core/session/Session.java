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

import com.datastax.oss.driver.api.core.cql.CqlSession;

/**
 * A nexus to send requests to a Cassandra cluster.
 *
 * <p>This is a high-level abstraction that can handle any kind of request (provided that you have
 * registered a custom request processor with the driver). For regular CQL queries, see {@link
 * CqlSession}.
 */
public interface Session {
  <SyncResultT, AsyncResultT> SyncResultT execute(Request<SyncResultT, AsyncResultT> request);

  <SyncResultT, AsyncResultT> AsyncResultT executeAsync(Request<SyncResultT, AsyncResultT> request);
}
