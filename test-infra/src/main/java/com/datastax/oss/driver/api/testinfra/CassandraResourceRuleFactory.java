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
package com.datastax.oss.driver.api.testinfra;

import com.datastax.oss.driver.api.testinfra.astra.AstraRule;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;

/**
 * Factory for creating {@link CassandraResourceRule} instances based on the {@code
 * ccm.distribution} system property.
 *
 * <p>This allows tests to run against different backends (Cassandra OSS, DSE, HCD, Astra) by simply
 * setting the {@code ccm.distribution} system property.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * @ClassRule
 * public static final CassandraResourceRule CASSANDRA_RESOURCE =
 *     CassandraResourceRuleFactory.getInstance();
 * }</pre>
 *
 * <p>To run against Cassandra OSS (default):
 *
 * <pre>
 * mvn test -Dtest=MyTest
 * </pre>
 *
 * <p>To run against Astra:
 *
 * <pre>
 * mvn test -Dtest=MyTest -Dccm.distribution=ASTRA -Dastra.token="..."
 * </pre>
 */
public class CassandraResourceRuleFactory {

  private static final BackendType DISTRIBUTION =
      BackendType.valueOf(
          System.getProperty("ccm.distribution", BackendType.CASSANDRA.name()).toUpperCase());

  /**
   * Returns a {@link CassandraResourceRule} instance based on the {@code ccm.distribution} system
   * property.
   *
   * <p>If {@code ccm.distribution=ASTRA}, returns {@link AstraRule#getInstance()}. Otherwise,
   * returns {@link CcmRule#getInstance()}.
   *
   * @return the appropriate CassandraResourceRule for the configured distribution
   */
  public static CassandraResourceRule getInstance() {
    return DISTRIBUTION == BackendType.ASTRA ? AstraRule.getInstance() : CcmRule.getInstance();
  }

  /**
   * Returns the configured backend distribution type.
   *
   * @return the BackendType from the ccm.distribution system property
   */
  public static BackendType getDistribution() {
    return DISTRIBUTION;
  }
}
