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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.cql.DefaultPreparedStatement;
import com.datastax.oss.protocol.internal.request.Prepare;
import java.nio.ByteBuffer;
import java.util.Map;
import net.jcip.annotations.Immutable;

/**
 * The information that's necessary to reprepare an already prepared statement, in case we hit a
 * node that doesn't have it in its cache.
 *
 * <p>Make sure the object that's returned to the client (e.g. {@link DefaultPreparedStatement} for
 * CQL statements) keeps a reference to this.
 */
@Immutable
public class RepreparePayload {
  public final ByteBuffer id;
  public final String query;

  /** The keyspace that is set independently from the query string (see CASSANDRA-10145) */
  public final CqlIdentifier keyspace;

  public final Map<String, ByteBuffer> customPayload;

  public RepreparePayload(
      ByteBuffer id, String query, CqlIdentifier keyspace, Map<String, ByteBuffer> customPayload) {
    this.id = id;
    this.query = query;
    this.keyspace = keyspace;
    this.customPayload = customPayload;
  }

  public Prepare toMessage() {
    return new Prepare(query, keyspace == null ? null : keyspace.asInternal());
  }
}
