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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableById;
import com.datastax.oss.driver.api.core.data.SettableByName;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * A prepared statement in its executable form, with values bound to the variables.
 *
 * <p>The default implementation provided by the driver is:
 *
 * <ul>
 *   <li>not thread-safe: all methods (setting values, etc.) must be called from the thread that
 *       created the instance. Besides, if you use {@link CqlSession#executeAsync(Statement)}
 *       asynchronous execution}, do not reuse the same instance for multiple calls, as you run the
 *       risk of modifying the statement while the driver internals are still processing it.
 *   <li>mutable: all setters that return a {@code BoundStatement} modify and return the same
 *       instance, instead of creating a copy.
 * </ul>
 */
public interface BoundStatement
    extends BatchableStatement,
        GettableById,
        GettableByName,
        SettableById<BoundStatement>,
        SettableByName<BoundStatement> {

  /** The prepared statement that was used to create this statement. */
  PreparedStatement getPreparedStatement();

  /** The values to bind, in their serialized form. */
  List<ByteBuffer> getValues();

  /**
   * Sets the name of the configuration profile to use.
   *
   * <p>Note that this will be ignored if {@link #getConfigProfile()} return a non-null value.
   */
  BoundStatement setConfigProfileName(String configProfileName);

  /** Sets the configuration profile to use. */
  BoundStatement setConfigProfile(DriverConfigProfile configProfile);

  /** Sets the custom payload to send alongside the request. */
  BoundStatement setCustomPayload(Map<String, ByteBuffer> customPayload);

  /**
   * Indicates whether the statement is idempotent.
   *
   * @param idempotent true or false, or {@code null} to use the default defined in the
   *     configuration.
   */
  BoundStatement setIdempotent(Boolean idempotent);

  /** Request tracing information for this statement. */
  BoundStatement setTracing();
}
