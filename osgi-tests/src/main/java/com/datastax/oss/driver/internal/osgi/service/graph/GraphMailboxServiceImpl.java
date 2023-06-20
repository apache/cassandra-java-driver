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
package com.datastax.oss.driver.internal.osgi.service.graph;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.osgi.service.MailboxException;
import com.datastax.oss.driver.api.osgi.service.MailboxMessage;
import com.datastax.oss.driver.api.osgi.service.graph.GraphMailboxService;
import com.datastax.oss.driver.internal.osgi.service.MailboxServiceImpl;
import java.time.Instant;
import java.util.stream.Collectors;

public class GraphMailboxServiceImpl extends MailboxServiceImpl implements GraphMailboxService {

  private final String graphName;

  public GraphMailboxServiceImpl(CqlSession session, CqlIdentifier keyspace, String graphName) {
    super(session, keyspace);
    this.graphName = graphName;
  }

  @Override
  protected void createSchema() {
    super.createSchema();
    session.execute(
        ScriptGraphStatement.newInstance(
                String.format("system.graph('%s').ifExists().drop()", graphName))
            .setSystemQuery(true),
        ScriptGraphStatement.SYNC);
    session.execute(
        ScriptGraphStatement.newInstance(
                String.format("system.graph('%s').ifNotExists().coreEngine().create()", graphName))
            .setSystemQuery(true),
        ScriptGraphStatement.SYNC);
    session.execute(
        ScriptGraphStatement.newInstance(
            "schema.vertexLabel('message')"
                + ".partitionBy('recipient', Text)"
                + ".clusterBy('timestamp', Timestamp)"
                + ".property('sender', Text)"
                + ".property('body', Text)"
                + ".create();"));
  }

  @Override
  public Iterable<MailboxMessage> getGraphMessages(String recipient) throws MailboxException {
    FluentGraphStatement statement =
        FluentGraphStatement.newInstance(
            g.V().hasLabel("message").has("recipient", recipient).valueMap().by(unfold()));
    try {
      return session.execute(statement).all().stream()
          .map(GraphNode::asMap)
          .map(
              vertex -> {
                Instant timestamp = (Instant) vertex.get("timestamp");
                String sender = (String) vertex.get("sender");
                String body = (String) vertex.get("body");
                return new MailboxMessage(recipient, timestamp, sender, body);
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }

  @Override
  public void sendGraphMessage(MailboxMessage message) throws MailboxException {
    FluentGraphStatement insertVertex =
        FluentGraphStatement.newInstance(
            g.addV("message")
                .property("recipient", message.getRecipient())
                .property("timestamp", message.getTimestamp())
                .property("sender", message.getSender())
                .property("body", message.getBody()));
    try {
      session.execute(insertVertex);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }
}
