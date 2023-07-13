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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Creates a keyspace and table, and loads data using an async API.
 *
 * <p>This example makes usage of a {@link CqlSession#executeAsync(String)} method, which is
 * responsible for executing requests in a non-blocking way. It uses {@link CompletableFuture} to
 * limit number of concurrent request to {@code CONCURRENCY_LEVEL}.
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster is running and accessible through the contact points
 *       identified by basic.contact-points (see application.conf).
 * </ul>
 *
 * <p>Side effects:
 *
 * <ul>
 *   <li>creates a new keyspace "examples" in the session. If a keyspace with this name already
 *       exists, it will be reused;
 *   <li>creates a table "examples.tbl_sample_kv". If it exist already, it will be reused;
 *   <li>inserts a TOTAL_NUMBER_OF_INSERTS of rows into the table.
 * </ul>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java Driver online manual</a>
 */
public class LimitConcurrencyCustomAsync {
  private static final int CONCURRENCY_LEVEL = 32;
  private static final int TOTAL_NUMBER_OF_INSERTS = 10_000;
  // Used to track number of total inserts
  private static final AtomicInteger INSERTS_COUNTER = new AtomicInteger();

  public static void main(String[] args) throws InterruptedException, ExecutionException {

    try (CqlSession session = new CqlSessionBuilder().build()) {
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

    // Construct CONCURRENCY_LEVEL number of ranges.
    // Each range will be executed independently.
    List<Range> ranges = createRanges(CONCURRENCY_LEVEL, TOTAL_NUMBER_OF_INSERTS);

    // List of pending CONCURRENCY_LEVEL features that we will wait for at the end of the program.
    List<CompletableFuture<?>> pending = new ArrayList<>();

    // Every range will have dedicated CompletableFuture handling the execution.
    for (Range range : ranges) {
      pending.add(executeOneAtATime(session, pst, range));
    }

    // Wait for completion of all CONCURRENCY_LEVEL pending CompletableFeatures
    CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).get();

    System.out.println(
        String.format(
            "Finished executing %s queries with a concurrency level of %s.",
            INSERTS_COUNTER.get(), CONCURRENCY_LEVEL));
  }

  private static CompletableFuture<?> executeOneAtATime(
      CqlSession session, PreparedStatement pst, Range range) {

    CompletableFuture<?> lastFeature = null;
    for (int i = range.getFrom(); i < range.getTo(); i++) {
      int counter = i;
      // If this is a first request init the lastFeature.
      if (lastFeature == null) {
        lastFeature = executeInsert(session, pst, counter);
      } else {
        // If lastFeature is already created, chain next async action.
        // The next action will execute only after the lastFeature will finish.
        // If the lastFeature finishes with failure, the subsequent chained executions
        // will not be invoked. If you wish to alter that behaviour and recover from failure
        // add the exceptionally() call after whenComplete() of lastFeature.
        lastFeature = lastFeature.thenCompose((ignored) -> executeInsert(session, pst, counter));
      }
    }
    return lastFeature;
  }

  private static CompletableFuture<? extends AsyncResultSet> executeInsert(
      CqlSession session, PreparedStatement pst, int counter) {

    return session
        .executeAsync(pst.bind().setUuid("id", Uuids.random()).setInt("value", counter))
        .toCompletableFuture()
        .whenComplete(
            (BiConsumer<AsyncResultSet, Throwable>)
                (asyncResultSet, throwable) -> {
                  if (throwable == null) {
                    // When the Feature completes and there is no exception - increment counter.
                    INSERTS_COUNTER.incrementAndGet();
                  } else {
                    // On production you should leverage logger and use logger.error() method.
                    throwable.printStackTrace();
                  }
                });
  }

  private static List<Range> createRanges(int concurrencyLevel, int totalNumberOfInserts) {
    ArrayList<Range> ranges = new ArrayList<>();
    int numberOfElementsInRange = totalNumberOfInserts / concurrencyLevel;
    // Create concurrencyLevel number of Ranges.
    for (int i = 0; i < concurrencyLevel; i++) {
      // If this is a last range give it all remaining elements.
      // It may be longer than numberOfElementsInRange in case of
      // totalNumberOfInserts / concurrencyLevel will return floating point number.
      if (i == concurrencyLevel - 1) {
        ranges.add(new Range(i * numberOfElementsInRange, totalNumberOfInserts));
      } else {
        // Construct Ranges with numberOfElementsInRange elements.
        ranges.add(new Range(i * numberOfElementsInRange, (i + 1) * numberOfElementsInRange));
      }
    }
    return ranges;
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.tbl_sample_kv (id uuid, value int, PRIMARY KEY (id))");
  }

  private static class Range {
    private final int from;
    private final int to;

    private Range(int from, int to) {
      this.from = from;
      this.to = to;
    }

    public int getFrom() {
      return from;
    }

    public int getTo() {
      return to;
    }

    @Override
    public String toString() {
      return "Range{" + "from=" + from + ", to=" + to + '}';
    }
  }
}
