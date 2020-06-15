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
package com.datastax.oss.driver.internal.osgi.service;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;

import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.osgi.service.MailboxException;
import com.datastax.oss.driver.api.osgi.service.MailboxMessage;
import com.datastax.oss.driver.api.osgi.service.MailboxService;
import java.util.Optional;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailboxServiceImpl implements MailboxService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxServiceImpl.class);

  protected final CqlSession session;
  protected final CqlIdentifier keyspace;

  @GuardedBy("this")
  protected boolean initialized = false;

  private PreparedStatement deleteStatement;

  protected MailboxMessageDao dao;

  public MailboxServiceImpl(CqlSession session, CqlIdentifier keyspace) {
    this.session = session;
    this.keyspace = keyspace;
  }

  public synchronized void init() {
    if (initialized) {
      return;
    }
    createSchema();
    prepareStatements();
    createDaos();
    printMetrics();
    initialized = true;
  }

  protected void createSchema() {
    session.execute("DROP KEYSPACE IF EXISTS test_osgi");
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_osgi with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
    session.execute(
        "CREATE TABLE "
            + keyspace
            + "."
            + MailboxMessage.MAILBOX_TABLE
            + " ("
            + "recipient text,"
            + "timestamp timestamp,"
            + "sender text,"
            + "body text,"
            + "PRIMARY KEY (recipient, timestamp))");
  }

  protected void prepareStatements() {
    deleteStatement =
        session.prepare(
            deleteFrom(keyspace, MailboxMessage.MAILBOX_TABLE)
                .where(column("recipient").isEqualTo(bindMarker()))
                .build());
  }

  protected void createDaos() {
    MailboxMapper mapper = new MailboxMapperBuilder(session).build();
    dao = mapper.mailboxMessageDao(keyspace);
  }

  protected void printMetrics() {
    // Exercise metrics
    if (session.getMetrics().isPresent()) {
      Metrics metrics = session.getMetrics().get();
      Optional<Timer> cqlRequests = metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS);
      cqlRequests.ifPresent(
          counter -> LOGGER.info("Number of CQL requests: {}", counter.getCount()));
    }
  }

  @Override
  public Iterable<MailboxMessage> getMessages(String recipient) throws MailboxException {
    try {
      return dao.findByRecipient(recipient);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }

  @Override
  public void sendMessage(MailboxMessage message) throws MailboxException {
    try {
      dao.save(message);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }

  @Override
  public void clearMailbox(String recipient) throws MailboxException {
    try {
      BoundStatement statement = deleteStatement.bind(recipient);
      session.execute(statement);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }
}
