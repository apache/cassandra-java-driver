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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A request to prepare a CQL query.
 *
 * <p>Driver clients should rarely have to deal directly with this type, it's used internally by
 * {@link Session}'s prepare methods. However a {@link RetryPolicy} implementation might use it if
 * it needs a custom behavior for prepare requests.
 */
public interface PrepareRequest extends Request {

  /**
   * The type returned when a CQL statement is prepared synchronously.
   *
   * <p>Most users won't use this explicitly. It is needed for the generic execute method ({@link
   * Session#execute(Request, GenericType)}), but CQL statements will generally be prepared with one
   * of the driver's built-in helper methods (such as {@link CqlSession#prepare(SimpleStatement)}).
   */
  GenericType<PreparedStatement> SYNC = GenericType.of(PreparedStatement.class);

  /**
   * The type returned when a CQL statement is prepared asynchronously.
   *
   * <p>Most users won't use this explicitly. It is needed for the generic execute method ({@link
   * Session#execute(Request, GenericType)}), but CQL statements will generally be prepared with one
   * of the driver's built-in helper methods (such as {@link
   * CqlSession#prepareAsync(SimpleStatement)}.
   */
  GenericType<CompletionStage<PreparedStatement>> ASYNC =
      new GenericType<CompletionStage<PreparedStatement>>() {};

  /** The CQL query to prepare. */
  @NonNull
  String getQuery();

  /**
   * {@inheritDoc}
   *
   * <p>Note that this refers to the prepare query itself, not to the bound statements that will be
   * created from the prepared statement (see {@link #areBoundStatementsIdempotent()}).
   */
  @NonNull
  @Override
  default Boolean isIdempotent() {
    // Retrying to prepare is always safe
    return true;
  }

  @Override
  default boolean isTracing() {
    // Tracing prepare requests is unlikely to be useful, we don't expose an API for it.
    return false;
  }

  /**
   * The name of the driver configuration profile to use for the bound statements that will be
   * created from the prepared statement.
   *
   * <p>Note that this will be ignored if {@link #getConfigProfileForBoundStatements()} returns a
   * non-null value.
   */
  @Nullable
  String getConfigProfileNameForBoundStatements();

  /**
   * The configuration profile to use for the bound statements that will be created from the
   * prepared statement.
   */
  @Nullable
  DriverConfigProfile getConfigProfileForBoundStatements();

  /**
   * Returns the custom payload to send alongside the bound statements that will be created from the
   * prepared statement.
   */
  @NonNull
  Map<String, ByteBuffer> getCustomPayloadForBoundStatements();

  /**
   * Whether bound statements that will be created from the prepared statement are idempotent.
   *
   * <p>This follows the same semantics as {@link #isIdempotent()}.
   */
  @Nullable
  Boolean areBoundStatementsIdempotent();

  /**
   * The page size to use for the bound statements that will be created from the prepared statement.
   * If the value is 0 or negative, the default value will be used from the configuration.
   */
  int getPageSizeForBoundStatements();

  /**
   * The consistency level to use for the bound statements that will be created from the prepared
   * statement or {@link null} to use the default value from the configuration.
   */
  @Nullable
  ConsistencyLevel getConsistencyLevelForBoundStatements();

  /**
   * The serial consistency level to use for the bound statements that will be created from the
   * prepared statement or {@code null} to use the default value from the configuration.
   */
  @Nullable
  ConsistencyLevel getSerialConsistencyLevelForBoundStatements();
}
