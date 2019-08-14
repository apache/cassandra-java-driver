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
package com.datastax.oss.driver.api.core.failover;

import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * The default failover policy.
 *
 * <p>To activate this policy, modify the {@code advanced.failover-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.failover-policy {
 *     class = DefaultFailoverPolicy
 *     profile = failoverProfileName
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
public interface FailoverPolicy extends AutoCloseable {

  /**
   * Whether to failover when throwable is thrown.
   *
   * @param throwable thrown throwable
   * @param request the request that threw throwable
   */
  Boolean shouldFailover(Throwable throwable, Statement<?> request);

  /**
   * Process the request object to adjust for the failover
   *
   * @param request request to be adjusted
   * @return adjusted request
   */
  Statement processRequest(Statement<?> request);
}
