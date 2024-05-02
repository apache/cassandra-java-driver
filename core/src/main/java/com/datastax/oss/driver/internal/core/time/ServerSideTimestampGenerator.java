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
package com.datastax.oss.driver.internal.core.time;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import net.jcip.annotations.ThreadSafe;

/**
 * A timestamp generator that never sends a timestamp with any query, therefore letting Cassandra
 * assign a server-side timestamp.
 *
 * <p>To activate this generator, modify the {@code advanced.timestamp-generator} section in the
 * driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.timestamp-generator {
 *     class = ServerSideTimestampGenerator
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class ServerSideTimestampGenerator implements TimestampGenerator {

  public ServerSideTimestampGenerator(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public long next() {
    return Statement.NO_DEFAULT_TIMESTAMP;
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
