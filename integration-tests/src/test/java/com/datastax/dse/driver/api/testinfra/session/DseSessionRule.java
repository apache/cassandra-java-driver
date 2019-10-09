/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.testinfra.session;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class DseSessionRule extends SessionRule<DseSession> {

  private final String graphName;

  public DseSessionRule(
      CassandraResourceRule cassandraResource,
      boolean createKeyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      DriverConfigLoader configLoader,
      String graphName) {
    super(cassandraResource, createKeyspace, nodeStateListener, schemaChangeListener, configLoader);
    this.graphName = graphName;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return super.apply(base, description);
  }

  @Override
  protected void before() {
    super.before();
    if (graphName != null) {
      session()
          .execute(
              ScriptGraphStatement.newInstance(
                      String.format("system.graph('%s').ifNotExists().create()", this.graphName))
                  .setSystemQuery(true));
    }
  }

  @Override
  protected void after() {
    if (graphName != null) {
      session()
          .execute(
              ScriptGraphStatement.newInstance(
                      String.format("system.graph('%s').drop()", this.graphName))
                  .setSystemQuery(true));
    }
    super.after();
  }

  public String getGraphName() {
    return graphName;
  }
}
