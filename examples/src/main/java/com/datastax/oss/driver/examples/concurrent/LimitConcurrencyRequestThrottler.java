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
package com.datastax.oss.driver.examples.concurrent;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Creates a keyspace and tables, and loads data using Async API into them.
 *
 * <p>This example makes usage of a {@link CqlSession#executeAsync(String)} method, which is
 * responsible for executing requests in a non-blocking way. It uses {@link
 * ConcurrencyLimitingRequestThrottler} to limit number of concurrent requests to 32. It uses
 * advanced.throttler configuration to limit async concurrency (max-concurrent-requests = 32) The
 * max-queue-size is set to 10000 to buffer {@code TOTAL_NUMBER_OF_INSERTS} in a queue in a case of
 * initial delay. (see application.conf)
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster is running and accessible through the contacts points
 *       identified by basic.contact-points (see application.conf).
 * </ul>
 *
 * <p>Side effects:
 *
 * <ul>
 *   <li>creates a new keyspace "examples" in the session. If a keyspace with this name already
 *       exists, it will be reused;
 *   <li>creates a table "examples.tbl_sample_kv". If it exists already, it will be reused;
 *   <li>inserts {@code TOTAL_NUMBER_OF_INSERTS} rows into the table.
 * </ul>
 *
 * @see <a
 *     href="https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/throttling/">Java
 *     driver online manual: Request throttling</a>
 */
public class LimitConcurrencyRequestThrottler {
  private static final int TOTAL_NUMBER_OF_INSERTS = 10_000;

  public static void main(String[] args) throws InterruptedException, ExecutionException {

    try (CqlSession session = CqlSession.builder().build()) {
      createSchema(session);
      insertConcurrent(session);
    }
  }

  private static void insertConcurrent(CqlSession session)
      throws InterruptedException, ExecutionException {
    PreparedStatement pst =
        session.prepare(
            insertInto("examples", "tbl_sample_kv")
                .value("id", bindMarker("id"))
                .value("value", bindMarker("value"))
                .build());
    // Create list of pending CompletableFutures.
    // We will add every operation returned from executeAsync.
    // Next, we will wait for completion of all TOTAL_NUMBER_OF_INSERTS
    List<CompletableFuture<?>> pending = new ArrayList<>();

    // For every i we will insert a record to db
    for (int i = 0; i < TOTAL_NUMBER_OF_INSERTS; i++) {
      pending.add(
          session
              .executeAsync(pst.bind().setUuid("id", Uuids.random()).setInt("value", i))
              // Transform CompletionState toCompletableFuture to be able to wait for execution of
              // all using CompletableFuture.allOf
              .toCompletableFuture());
    }

    // Wait for completion of all TOTAL_NUMBER_OF_INSERTS pending requests
    CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).get();

    System.out.println(
        String.format(
            "Finished executing %s queries with a concurrency level of %s.",
            pending.size(),
            session
                .getContext()
                .getConfig()
                .getDefaultProfile()
                .getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS)));
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.tbl_sample_kv (id uuid, value int, PRIMARY KEY (id))");
  }
}
