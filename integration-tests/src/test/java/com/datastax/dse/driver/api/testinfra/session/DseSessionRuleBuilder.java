/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.testinfra.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.config.typesafe.DefaultDseDriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRuleBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class DseSessionRuleBuilder extends SessionRuleBuilder<DseSessionRuleBuilder, DseSession> {

  private static final AtomicInteger GRAPH_NAME_INDEX = new AtomicInteger();

  private boolean createGraph;

  public DseSessionRuleBuilder(CassandraResourceRule cassandraResource) {
    super(cassandraResource);
  }

  /**
   * Configures the rule to create a new graph instance.
   *
   * <p>This assumes that the associated {@link CassandraResourceRule} is a DSE instance with the
   * graph workload enabled.
   *
   * <p>The name of the graph will be injected in the session's configuration, so that all graph
   * statements are automatically routed to it. It's also exposed via {@link
   * DseSessionRule#getGraphName()}.
   */
  public DseSessionRuleBuilder withCreateGraph() {
    this.createGraph = true;
    return this;
  }

  @Override
  public DseSessionRule build() {
    final String graphName;
    final DriverConfigLoader actualLoader;
    if (createGraph) {
      graphName = "dsedrivertests_" + GRAPH_NAME_INDEX.getAndIncrement();

      // Inject the generated graph name in the provided configuration, so that the test doesn't
      // need to set it explicitly on every statement.
      if (loader == null) {
        // This would normally be handled in DseSessionBuilder, do it early because we need it now
        loader = new DefaultDseDriverConfigLoader();
      } else {
        // To keep this relatively simple we assume that if the config loader was provided in a
        // test, it is the Typesafe-config based one. This is always true in our integration tests.
        assertThat(loader).isInstanceOf(DefaultDriverConfigLoader.class);
      }
      Supplier<Config> originalSupplier = ((DefaultDriverConfigLoader) loader).getConfigSupplier();
      Supplier<Config> actualSupplier =
          () ->
              originalSupplier
                  .get()
                  .withValue(
                      DseDriverOption.GRAPH_NAME.getPath(),
                      ConfigValueFactory.fromAnyRef(graphName));
      actualLoader = new DefaultDseDriverConfigLoader(actualSupplier);
    } else {
      graphName = null;
      actualLoader = loader;
    }

    return new DseSessionRule(
        cassandraResource,
        createKeyspace,
        nodeStateListener,
        schemaChangeListener,
        actualLoader,
        graphName);
  }
}
