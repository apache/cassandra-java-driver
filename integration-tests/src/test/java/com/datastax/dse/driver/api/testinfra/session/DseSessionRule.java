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
