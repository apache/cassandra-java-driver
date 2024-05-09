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
package com.datastax.oss.driver.examples.retry;

import static com.datastax.oss.driver.api.core.DefaultConsistencyLevel.QUORUM;
import static com.datastax.oss.driver.api.core.cql.DefaultBatchType.UNLOGGED;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.QueryConsistencyException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import java.util.List;

/**
 * This example illustrates how to implement a downgrading retry strategy from application code.
 *
 * <p>This strategy is equivalent to the logic implemented by the consistency downgrading retry
 * policy, but we think that such a logic should be implemented at application level whenever
 * possible.
 *
 * <p>See the <a
 * href="https://docs.datastax.com/en/developer/java-driver/latest/faq/#where-is-downgrading-consistency-retry-policy">FAQ</a>
 * and the <a
 * href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries">manual
 * section on retries</a>.
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
 * <ol>
 *   <li>Creates a new keyspace {@code downgrading} in the cluster, with replication factor 3. If a
 *       keyspace with this name already exists, it will be reused;
 *   <li>Creates a new table {@code downgrading.sensor_data}. If a table with that name exists
 *       already, it will be reused;
 *   <li>Inserts a few rows, downgrading the consistency level if the operation fails;
 *   <li>Queries the table, downgrading the consistency level if the operation fails;
 *   <li>Displays the results on the console.
 * </ol>
 *
 * Notes:
 *
 * <ul>
 *   <li>The downgrading logic here is similar to what {@code DowngradingConsistencyRetryPolicy} in
 *       3.x driver does; feel free to adapt it to your application needs;
 *   <li>You should never attempt to retry a non-idempotent write. See the driver's manual page on
 *       idempotence for more information.
 * </ul>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.0">Java Driver online
 *     manual</a>
 */
public class DowngradingRetry {
  /** The maximum number of retries to attempt. */
  private static final int MAX_RETRIES = 1;

  /** The initial consistency level to use. */
  private static final ConsistencyLevel INITIAL_CL = QUORUM;

  public static void main(String[] args) {

    DowngradingRetry client = new DowngradingRetry(MAX_RETRIES);

    try {

      client.connect();
      client.createSchema();
      client.write(INITIAL_CL, 0);
      ResultSet rows = client.read(INITIAL_CL, 0);
      client.display(rows);

    } finally {
      client.close();
    }
  }

  private final int maxRetries;

  private CqlSession session;

  private DowngradingRetry(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /** Initiates a connection to the session specified by the application.conf. */
  private void connect() {
    session = CqlSession.builder().build();

    System.out.printf("Connected to session: %s%n", session.getName());
  }

  /** Creates the schema (keyspace) and table for this example. */
  private void createSchema() {

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS downgrading WITH replication "
            + "= {'class':'SimpleStrategy', 'replication_factor':3}");

    session.execute(
        "CREATE TABLE IF NOT EXISTS downgrading.sensor_data ("
            + "sensor_id uuid,"
            + "date date,"
            + // emulates bucketing by day
            "timestamp timestamp,"
            + "value double,"
            + "PRIMARY KEY ((sensor_id,date),timestamp)"
            + ")");
  }

  /**
   * Inserts data, retrying if necessary with a downgraded CL.
   *
   * @param cl the consistency level to apply.
   * @param retryCount the current retry count.
   * @throws DriverException if the current consistency level cannot be downgraded.
   */
  private void write(ConsistencyLevel cl, int retryCount) {

    System.out.printf("Writing at %s (retry count: %d)%n", cl, retryCount);

    BatchStatement batch =
        BatchStatement.newInstance(UNLOGGED)
            .add(
                SimpleStatement.newInstance(
                    "INSERT INTO downgrading.sensor_data "
                        + "(sensor_id, date, timestamp, value) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'2018-02-26',"
                        + "'2018-02-26T13:53:46.345+01:00',"
                        + "2.34)"))
            .add(
                SimpleStatement.newInstance(
                    "INSERT INTO downgrading.sensor_data "
                        + "(sensor_id, date, timestamp, value) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'2018-02-26',"
                        + "'2018-02-26T13:54:27.488+01:00',"
                        + "2.47)"))
            .add(
                SimpleStatement.newInstance(
                    "INSERT INTO downgrading.sensor_data "
                        + "(sensor_id, date, timestamp, value) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'2018-02-26',"
                        + "'2018-02-26T13:56:33.739+01:00',"
                        + "2.52)"))
            .setConsistencyLevel(cl);

    try {

      session.execute(batch);
      System.out.println("Write succeeded at " + cl);

    } catch (DriverException e) {

      if (retryCount == maxRetries) {
        throw e;
      }

      e = unwrapAllNodesFailedException(e);

      System.out.println("Write failed: " + e);

      // General intent:
      // 1) If we know the write has been fully persisted on at least one replica,
      // ignore the exception since the write will be eventually propagated to other replicas.
      // 2) If the write couldn't be persisted at all, abort as it is unlikely that a retry would
      // succeed.
      // 3) If the write was only partially persisted, retry at the highest consistency
      // level that is likely to succeed.

      if (e instanceof UnavailableException) {

        // With an UnavailableException, we know that the write wasn't even attempted.
        // Downgrade to the number of replicas reported alive and retry.
        int aliveReplicas = ((UnavailableException) e).getAlive();

        ConsistencyLevel downgraded = downgrade(cl, aliveReplicas, e);
        write(downgraded, retryCount + 1);

      } else if (e instanceof WriteTimeoutException) {

        DefaultWriteType writeType = (DefaultWriteType) ((WriteTimeoutException) e).getWriteType();
        int acknowledgements = ((WriteTimeoutException) e).getReceived();

        switch (writeType) {
          case SIMPLE:
          case BATCH:
            // For simple and batch writes, as long as one replica acknowledged the write,
            // ignore the exception; if none responded however, abort as it is unlikely that
            // a retry would ever succeed.
            if (acknowledgements == 0) {
              throw e;
            }
            break;

          case UNLOGGED_BATCH:
            // For unlogged batches, the write might have been persisted only partially,
            // so we can't simply ignore the exception: instead, we need to retry with
            // consistency level equal to the number of acknowledged writes.
            ConsistencyLevel downgraded = downgrade(cl, acknowledgements, e);
            write(downgraded, retryCount + 1);
            break;

          case BATCH_LOG:
            // Rare edge case: the peers that were chosen by the coordinator
            // to receive the distributed batch log failed to respond.
            // Simply retry with same consistency level.
            write(cl, retryCount + 1);
            break;

          default:
            // Other write types are uncommon and should not be retried.
            throw e;
        }

      } else {

        // Unexpected error: just retry with same consistency level
        // and hope to talk to a healthier coordinator.
        write(cl, retryCount + 1);
      }
    }
  }

  /**
   * Queries data, retrying if necessary with a downgraded CL.
   *
   * @param cl the consistency level to apply.
   * @param retryCount the current retry count.
   * @throws DriverException if the current consistency level cannot be downgraded.
   */
  private ResultSet read(ConsistencyLevel cl, int retryCount) {

    System.out.printf("Reading at %s (retry count: %d)%n", cl, retryCount);

    Statement stmt =
        SimpleStatement.newInstance(
                "SELECT sensor_id, date, timestamp, value "
                    + "FROM downgrading.sensor_data "
                    + "WHERE "
                    + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                    + "date = '2018-02-26' AND "
                    + "timestamp > '2018-02-26+01:00'")
            .setConsistencyLevel(cl);

    try {

      ResultSet rows = session.execute(stmt);
      System.out.println("Read succeeded at " + cl);
      return rows;

    } catch (DriverException e) {

      if (retryCount == maxRetries) {
        throw e;
      }

      e = unwrapAllNodesFailedException(e);

      System.out.println("Read failed: " + e);

      // General intent: downgrade and retry at the highest consistency level
      // that is likely to succeed.

      if (e instanceof UnavailableException) {

        // Downgrade to the number of replicas reported alive and retry.
        int aliveReplicas = ((UnavailableException) e).getAlive();

        ConsistencyLevel downgraded = downgrade(cl, aliveReplicas, e);
        return read(downgraded, retryCount + 1);

      } else if (e instanceof ReadTimeoutException) {

        ReadTimeoutException readTimeout = (ReadTimeoutException) e;
        int received = readTimeout.getReceived();
        int required = readTimeout.getBlockFor();

        // If fewer replicas responded than required by the consistency level
        // (but at least one replica did respond), retry with a consistency level
        // equal to the number of received acknowledgements.
        if (received < required) {

          ConsistencyLevel downgraded = downgrade(cl, received, e);
          return read(downgraded, retryCount + 1);
        }

        // If we received enough replies to meet the consistency level,
        // but the actual data was not present among the received responses,
        // then retry with the initial consistency level, we might be luckier next time
        // and get the data back.
        if (!readTimeout.wasDataPresent()) {

          return read(cl, retryCount + 1);
        }

        // Otherwise, abort since the read timeout is unlikely to be solved by a retry.
        throw e;

      } else {

        // Unexpected error: just retry with same consistency level
        // and hope to talk to a healthier coordinator.
        return read(cl, retryCount + 1);
      }
    }
  }

  /**
   * Displays the results on the console.
   *
   * @param rows the results to display.
   */
  private void display(ResultSet rows) {

    final int width1 = 38;
    final int width2 = 12;
    final int width3 = 30;
    final int width4 = 21;

    String format = "%-" + width1 + "s%-" + width2 + "s%-" + width3 + "s%-" + width4 + "s%n";

    // headings
    System.out.printf(format, "sensor_id", "date", "timestamp", "value");

    // separators
    drawLine(width1, width2, width3, width4);

    // data
    for (Row row : rows) {

      System.out.printf(
          format,
          row.getUuid("sensor_id"),
          row.getLocalDate("date"),
          row.getInstant("timestamp"),
          row.getDouble("value"));
    }
  }

  /** Closes the session and the cluster. */
  private void close() {
    if (session != null) {
      session.close();
    }
  }

  /**
   * Downgrades the current consistency level to the highest level that is likely to succeed, given
   * the number of acknowledgements received. Rethrows the original exception if the current
   * consistency level cannot be downgraded any further.
   *
   * @param current the current CL.
   * @param acknowledgements the acknowledgements received.
   * @param original the original exception.
   * @return the downgraded CL.
   * @throws DriverException if the current consistency level cannot be downgraded.
   */
  private static ConsistencyLevel downgrade(
      ConsistencyLevel current, int acknowledgements, DriverException original) {
    if (acknowledgements >= 3) {
      return DefaultConsistencyLevel.THREE;
    }
    if (acknowledgements == 2) {
      return DefaultConsistencyLevel.TWO;
    }
    if (acknowledgements == 1) {
      return DefaultConsistencyLevel.ONE;
    }
    // Edge case: EACH_QUORUM does not report a global number of alive replicas
    // so even if we get 0 alive replicas, there might be
    // a node up in some other datacenter, so retry at ONE.
    if (current == DefaultConsistencyLevel.EACH_QUORUM) {
      return DefaultConsistencyLevel.ONE;
    }
    throw original;
  }

  /**
   * If the driver was unable to contact any node, it throws an umbrella {@link
   * NoNodeAvailableException} containing a map of the actual errors, keyed by host.
   *
   * <p>This method unwraps this exception, inspects the map of errors, and returns the first
   * exploitable {@link DriverException}.
   *
   * @param e the exception to unwrap.
   * @return the unwrapped exception, or the original exception, if it is not an instance of {@link
   *     NoNodeAvailableException}.
   * @throws NoNodeAvailableException the original exception, if it cannot be unwrapped.
   */
  private static DriverException unwrapAllNodesFailedException(DriverException e) {
    if (e instanceof AllNodesFailedException) {
      AllNodesFailedException noHostAvailable = (AllNodesFailedException) e;
      for (List<Throwable> errors : noHostAvailable.getAllErrors().values()) {
        for (Throwable error : errors) {
          if (error instanceof QueryConsistencyException || error instanceof UnavailableException) {
            return (DriverException) error;
          }
        }
      }
      // Couldn't find an exploitable error to unwrap: abort.
      throw e;
    }
    // the original exceptional wasn't a NoHostAvailableException: proceed.
    return e;
  }

  /**
   * Draws a line to isolate headings from rows.
   *
   * @param widths the column widths.
   */
  private static void drawLine(int... widths) {
    for (int width : widths) {
      for (int i = 1; i < width; i++) {
        System.out.print('-');
      }
      System.out.print('+');
    }
    System.out.println();
  }
}
