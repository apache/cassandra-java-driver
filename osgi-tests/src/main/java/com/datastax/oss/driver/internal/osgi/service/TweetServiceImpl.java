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
package com.datastax.oss.driver.internal.osgi.service;

import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.osgi.service.TweetMessage;
import com.datastax.oss.driver.api.osgi.service.TweetService;
import java.util.Optional;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TweetServiceImpl implements TweetService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TweetServiceImpl.class);

  protected final CqlSession session;
  protected final CqlIdentifier keyspace;

  @GuardedBy("this")
  protected boolean initialized = false;

  private PreparedStatement insertStatement;

  public TweetServiceImpl(CqlSession session, CqlIdentifier keyspace) {
    this.session = session;
    this.keyspace = keyspace;
  }

  public synchronized void init() {
    if (initialized) {
      return;
    }
    createSchema();
    prepareStatements();
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
            + ".tweets ("
            + "sender text,"
            + "timestamp timestamp,"
            + "body text,"
            + "PRIMARY KEY (sender, timestamp))");
  }

  protected void prepareStatements() {
    insertStatement =
        session.prepare(
            "INSERT INTO " + keyspace + ".tweets(sender, timestamp, body) VALUES (?, ?, ?)");
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
  public void sendMessage(TweetMessage message) {
    try {
      BoundStatement statement =
          insertStatement.bind(message.getSender(), message.getTimestamp(), message.getBody());
      session.execute(statement);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
