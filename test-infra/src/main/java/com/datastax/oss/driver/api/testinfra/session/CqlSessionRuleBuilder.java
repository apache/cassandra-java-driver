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
package com.datastax.oss.driver.api.testinfra.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CqlSessionRuleBuilder extends SessionRuleBuilder<CqlSessionRuleBuilder, CqlSession> {

  private static final AtomicInteger GRAPH_NAME_INDEX = new AtomicInteger();

  public CqlSessionRuleBuilder(CassandraResourceRule cassandraResource) {
    super(cassandraResource);
  }

  @Override
  public SessionRule<CqlSession> build() {

    final String graphName;
    final DriverConfigLoader actualLoader;

    Supplier<Config> actualSupplier;

    if (createGraph) {
      graphName = "dsedrivertests_" + GRAPH_NAME_INDEX.getAndIncrement();

      // Inject the generated graph name in the provided configuration, so that the test doesn't
      // need to set it explicitly on every statement.
      if (loader == null) {
        // This would normally be handled in DseSessionBuilder, do it early because we need it now
        loader = new DefaultDriverConfigLoader();
      } else {
        // To keep this relatively simple we assume that if the config loader was provided in a
        // test, it is the Typesafe-config based one. This is always true in our integration tests.
        assertThat(loader).isInstanceOf(DefaultDriverConfigLoader.class);
      }
      Supplier<Config> originalSupplier = ((DefaultDriverConfigLoader) loader).getConfigSupplier();
      actualSupplier =
          () ->
              originalSupplier
                  .get()
                  .withValue(
                      DseDriverOption.GRAPH_NAME.getPath(),
                      ConfigValueFactory.fromAnyRef(graphName));
    } else {
      graphName = null;
      if (loader == null) {
        loader = new DefaultDriverConfigLoader();
      }

      actualSupplier = ((DefaultDriverConfigLoader) loader).getConfigSupplier();
    }

    actualLoader =
        new DefaultDriverConfigLoader(
            () ->
                graphProtocol != null
                    ? actualSupplier
                        .get()
                        .withValue(
                            DseDriverOption.GRAPH_SUB_PROTOCOL.getPath(),
                            ConfigValueFactory.fromAnyRef(graphProtocol))
                    // will use the protocol from the config file (in application.conf if
                    // defined or in reference.conf)
                    : actualSupplier.get());

    return new SessionRule<>(
        cassandraResource,
        createKeyspace,
        nodeStateListener,
        schemaChangeListener,
        actualLoader,
        graphName,
        isCoreGraph);
  }
}
