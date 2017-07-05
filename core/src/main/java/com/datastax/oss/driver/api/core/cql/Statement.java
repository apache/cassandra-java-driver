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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/** A request to execute a CQL query. */
public interface Statement extends Request<ResultSet, CompletionStage<AsyncResultSet>> {
  // Implementation note: "CqlRequest" would be a better name, but we keep "Statement" to match
  // previous driver versions.

  /**
   * The query timestamp to send with the statement.
   *
   * <p>If this is equal to {@link Long#MIN_VALUE}, the {@link TimestampGenerator} configured for
   * this driver instance will be used to generate a timestamp.
   */
  long getTimestamp();

  ByteBuffer getPagingState();

  /** Creates a new statement with a different paging state. */
  // Implementation note: this is a simplistic first draft in order to move forward with paging,
  // however more thought is needed about statement attributes: which belong to the API and which
  // belong to DriverConfig, how you override them for a specific statement, whether statement
  // implementations are immutable, etc.
  Statement copy(ByteBuffer newPagingState);
}
