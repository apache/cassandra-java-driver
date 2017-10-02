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
package com.datastax.oss.driver.api.core.time;

import com.datastax.oss.driver.api.core.context.DriverContext;

/**
 * A timestamp generator that never sends a timestamp with any query, therefore letting Cassandra
 * assign a server-side timestamp.
 */
public class ServerSideTimestampGenerator implements TimestampGenerator {

  public ServerSideTimestampGenerator(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public long next() {
    return Long.MIN_VALUE;
  }
}
