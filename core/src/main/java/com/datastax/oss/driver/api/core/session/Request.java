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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A request executed by a {@link Session}.
 *
 * <p>This is a high-level abstraction, agnostic to the actual language (e.g. CQL). A request is
 * anything that can be converted to a protocol message, provided that you register a request
 * processor with the driver to do that conversion.
 */
public interface Request {

  /**
   * The name of the driver configuration profile that will be used for execution.
   *
   * <p>Note that this will be ignored if {@link #getConfigProfile()} returns a non-null value.
   *
   * @see DriverConfig
   */
  String getConfigProfileName();

  /**
   * The configuration profile to use for execution.
   *
   * <p>It is generally simpler to specify a profile name with {@link #getConfigProfileName()}.
   * However, this method can be used to provide a "derived" profile that was built programatically
   * by the client code. If specified, it overrides the profile name.
   *
   * @see DriverConfigProfile
   */
  DriverConfigProfile getConfigProfile();

  /**
   * <b>NOT YET SUPPORTED</b> -- the CQL keyspace to associate with the query.
   *
   * <p>This will be available when <a
   * href="https://issues.apache.org/jira/browse/CASSANDRA-10145">CASSANDRA-10145</a> is merged in a
   * stable server release. In the meantime, the method is present to avoid breaking the API later,
   * but returning any value other than {@code null} will cause a runtime exception.
   */
  String getKeyspace();

  /**
   * Returns the custom payload to send alongside the request.
   *
   * <p>This is used to exchange extra information with the server. By default, Cassandra doesn't do
   * anything with this, you'll only need it if you have a custom request handler on the
   * server-side.
   */
  Map<String, ByteBuffer> getCustomPayload();

  /**
   * Whether the request is idempotent; that is, whether applying the request twice yields the same
   * result.
   *
   * <p>This is used internally for retries and speculative executions: if a request is not
   * idempotent, the driver will take extra care to ensure that it is not sent twice (for example,
   * don't retry if there is the slightest chance that the request reached a coordinator).
   *
   * @return a boolean value, or {@code null} to use the default value defined in the configuration.
   * @see CoreDriverOption#REQUEST_DEFAULT_IDEMPOTENCE
   */
  Boolean isIdempotent();

  /**
   * Whether tracing information should be recorded for this request.
   *
   * <p>Tracing is rather specific to CQL, but this is exposed in this interface because it is
   * available at the protocol level. Request implementations are free to use it if it is relevant
   * to them, or always return {@code false} otherwise.
   */
  boolean isTracing();
}
