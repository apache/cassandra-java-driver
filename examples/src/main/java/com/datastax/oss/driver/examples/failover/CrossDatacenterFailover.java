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
package com.datastax.oss.driver.examples.failover;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.QueryConsistencyException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/**
 * This example illustrates how to implement a cross-datacenter failover strategy from application
 * code.
 *
 * <p>Starting with driver 4.10, cross-datacenter failover is also provided as a configuration
 * option for built-in load balancing policies. See <a
 * href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">Load
 * balancing</a> in the manual.
 *
 * <p>This example demonstrates how to achieve the same effect in application code, which confers
 * more fained-grained control over which statements should be retried and where.
 *
 * <p>The logic that decides whether or not a cross-DC failover should be attempted is presented in
 * the {@link #shouldFailover(DriverException)} method below; study it carefully and adapt it to
 * your needs if necessary.
 *
 * <p>The actual request execution and failover code is presented in 3 different programming styles:
 *
 * <ol>
 *   <li>Synchronous: see the {@link #writeSync()} method below;
 *   <li>Asynchronous: see the {@link #writeAsync()} method below;
 * </ol>
 * <p>
 * The 3 styles are identical in terms of failover effect; they are all included merely to help
 * programmers pick the variant that is closest to the style they use.
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster with two datacenters, dc1 and dc2, containing at least 3
 *       nodes in each datacenter, is running and accessible through the contact point:
 *       127.0.0.1:9042.
 * </ul>
 *
 * <p>Side effects:
 *
 * <ol>
 *   <li>Creates a new table {@code testks.orders}. If a table with that name exists already, it
 *       will be reused;
 *   <li>Tries to write a row in the table using the local datacenter dc1;
 *   <li>If the local datacenter dc1 is down, retries the write in the remote datacenter dc2.
 * </ol>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest">Java Driver online
 * manual</a>
 */
public class CrossDatacenterFailover {

    public static void main(String[] args) throws Exception {

        CrossDatacenterFailover client = new CrossDatacenterFailover();

        int numberOfWrites = Integer.parseInt(System.getProperty("num_writes", "1"));

        try {

            // Note: when this example is executed, at least the local DC must be available
            // since the driver will try to reach contact points in that DC.

            client.connect();
            client.createSchema();

            // To fully exercise this example, try to stop the entire dc1 here; then observe how
            // the writes executed below will first fail in dc1, then be diverted to dc2, where they will
            // succeed.

            Statement<?> statement = SimpleStatement.newInstance(
                    "INSERT INTO testks.orders "
                            + "(product_id, timestamp, price) "
                            + "VALUES ("
                            + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                            + "'2018-02-26T13:53:46.345+01:00',"
                            + "2.34)");

            for (int i = 0; i < numberOfWrites; i++) {

                //client.writeSync(statement);
                client.writeAsync(statement, i);
            }

        } finally {
            client.close();
        }
    }

    private CqlSession primarySession;
    private CqlSession secondarySession;

    private CrossDatacenterFailover() {
    }

    /**
     * Initiates a connection to the cluster.
     */
    private void connect() {
        // Load properties
        String rootPath = Paths.get(CrossDatacenterFailover.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent().toString() + "/";
        String astraConfigPath = rootPath + "astra.properties";

        Properties astraProp = new Properties();
        try {

            astraProp.load(new FileInputStream(astraConfigPath));

        } catch (IOException e) {

            System.out.println("Configuration file 'astra.properties' is missing or invalid.");
            e.printStackTrace();
        }

        // Configure these properties in "astra.properties"
        String primarySecureConnectBundle = astraProp.getProperty("primary_secure_connect_bundle");
        String secondarySecureConnectBundle = astraProp.getProperty("secondary_secure_connect_bundle");
        String keyspace = astraProp.getProperty("keyspace");

        // Get credentials from system properties
        String clientId = System.getProperty("astra_user", null);
        String clientSecret = System.getProperty("astra_password", null);

        if (clientId == null || clientSecret == null) {

            System.out.println("Astra credentials are missing. Use system properties -Dastra_user=<username> -Dastra_password=<password>");
            System.exit(1);
        }

        try {

            primarySession = CqlSession.builder()
                    .withCloudSecureConnectBundle(Paths.get(primarySecureConnectBundle))
                    .withAuthCredentials(clientId, clientSecret)
                    .withKeyspace(keyspace)
                    .build();

            System.out.println("Connected to cluster with session: " + primarySession.getName());

        } catch (AllNodesFailedException | IllegalStateException e) {

            System.out.println("Error connecting to primary session: " + e);
            primarySession = null;
        }

        try {

            secondarySession = CqlSession.builder()
                    .withCloudSecureConnectBundle(Paths.get(secondarySecureConnectBundle))
                    .withAuthCredentials(clientId, clientSecret)
                    .withKeyspace(keyspace)
                    .build();

            System.out.println("Connected to cluster with session: " + secondarySession.getName());

        } catch (AllNodesFailedException | IllegalStateException e) {

            System.out.println("Error connecting to secondary session: " + e);
            secondarySession = null;
        }

        if (primarySession == null && secondarySession == null) {

            System.out.println("Unable to connect to any session");
            System.exit(1);
        }
    }

    /**
     * Creates the schema (keyspace) and table for this example.
     */
    private void createSchema() {

        try {

            primarySession.execute(
                    "CREATE TABLE IF NOT EXISTS testks.orders ("
                            + "product_id uuid,"
                            + "timestamp timestamp,"
                            + "price double,"
                            + "PRIMARY KEY (product_id,timestamp)"
                            + ")");

        } catch (Exception e) {

            System.out.println("Error creating schema, retrying with remote DC");

            try {

                secondarySession.execute(
                        "CREATE TABLE IF NOT EXISTS testks.orders ("
                                + "product_id uuid,"
                                + "timestamp timestamp,"
                                + "price double,"
                                + "PRIMARY KEY (product_id,timestamp)"
                                + ")");

            } catch (Exception e2) {

                System.out.println("Error creating schema in remote DC");
                e2.printStackTrace();
            }

        }
    }

    /**
     * Inserts data synchronously using the local DC, retrying if necessary in a remote DC.
     */
    private void writeSync(Statement<?> statement) {

        System.out.println("------- DC failover (sync) ------- ");

        try {

            // try the statement using the default profile, which targets the local datacenter.
            primarySession.execute(statement);

            System.out.println("Write to local DC succeeded");

        } catch (DriverException e) {

            if (shouldFailover(e)) {

                System.out.println("Write failed in local DC, retrying in remote DC");

                try {

                    // try the statement using the secondary session, which targets the remote datacenter.
                    secondarySession.execute(statement);

                    System.out.println("Write to remote DC succeeded");

                } catch (DriverException e2) {

                    System.out.println("Write failed in remote DC");

                    e2.printStackTrace();
                }
            }
        }
        // let other errors propagate
    }

    /**
     * Inserts data asynchronously using the local DC, retrying if necessary in a remote DC.
     */
    private void writeAsync(Statement<?> statement, int iteratorCount) throws ExecutionException, InterruptedException {

        //System.out.println("------- DC failover (async) ------- ");

        CompletionStage<AsyncResultSet> result;

        if (primarySession != null) {
            result =
                    // try the statement using the primary session, which targets the local datacenter.
                    primarySession
                            .executeAsync(statement)
                            .handle(
                                    (rs, error) -> {
                                        if (error == null) {
                                            return CompletableFuture.completedFuture(rs);
                                        } else {
                                            if (error instanceof DriverException
                                                    && shouldFailover((DriverException) error)) {
                                                System.out.println("Write failed in local DC, retrying in remote DC");
                                                // try the statement using the secondary session, which targets the remote
                                                // datacenter.
                                                return secondarySession.executeAsync(statement);
                                            }
                                            // let other errors propagate
                                            return CompletableFutures.<AsyncResultSet>failedFuture(error);
                                        }
                                    })
                            // unwrap (flatmap) the nested future
                            .thenCompose(future -> future)
                            .whenComplete(
                                    (rs, error) -> {
                                        if (error == null) {
                                            if (iteratorCount % 100 == 0) {
                                                System.out.println("Write succeeded");
                                            }
                                        } else {
                                            System.out.println("Write failed in remote DC");
                                            error.printStackTrace();
                                        }
                                    });
        } else {
            result =
                    secondarySession
                            .executeAsync(statement)
                            .handle(
                                    (rs, error) -> {
                                        if (error == null) {
                                            return CompletableFuture.completedFuture(rs);
                                        } else {
                                            System.out.println("Write failed in remote DC");
                                            // let other errors propagate
                                            return CompletableFutures.<AsyncResultSet>failedFuture(error);
                                        }
                                    })
                            // unwrap (flatmap) the nested future
                            .thenCompose(future -> future)
                            .whenComplete(
                                    (rs, error) -> {
                                        if (error == null) {
                                            if (iteratorCount % 100 == 0) {
                                                System.out.println("Write succeeded");
                                            }
                                        } else {
                                            System.out.println("Write failed in remote DC");
                                            error.printStackTrace();
                                        }
                                    });
        }

        // for the sake of this example, wait for the operation to finish
        result.toCompletableFuture().get();
    }

    /**
     * Analyzes the error and decides whether to failover to a remote DC.
     *
     * <p>The logic below categorizes driver exceptions in four main groups:
     *
     * <ol>
     *   <li>Total DC outage: all nodes in DC were known to be down when the request was executed;
     *   <li>Partial DC outage: one or many nodes responded, but reported a replica availability
     *       problem;
     *   <li>DC unreachable: one or many nodes were queried, but none responded (timeout);
     *   <li>Other errors.
     * </ol>
     * <p>
     * A DC failover is authorized for the first three groups above: total DC outage, partial DC
     * outage, and DC unreachable.
     *
     * <p>This logic is provided as a good starting point for users to create their own DC failover
     * strategy; please adjust it to your exact needs.
     */
    private boolean shouldFailover(DriverException mainException) {

        if (mainException instanceof NoNodeAvailableException) {

            // No node could be tried, because all nodes in the query plan were down. This could be a
            // total DC outage, so trying another DC makes sense.
            System.out.println("All nodes were down in this datacenter, failing over");
            return true;

        } else if (mainException instanceof AllNodesFailedException) {

            // Many nodes were tried (as decided by the retry policy), but all failed. This could be a
            // partial DC outage: some nodes were up, but the replicas were down.

            boolean failover = false;

            // Inspect the error to find out how many coordinators were tried, and which errors they
            // returned.
            for (Entry<Node, List<Throwable>> entry :
                    ((AllNodesFailedException) mainException).getAllErrors().entrySet()) {

                Node coordinator = entry.getKey();
                List<Throwable> errors = entry.getValue();

                System.out.printf(
                        "Node %s in DC %s was tried %d times but failed with:%n",
                        coordinator.getEndPoint(), coordinator.getDatacenter(), errors.size());

                for (Throwable nodeException : errors) {

                    System.out.printf("\t- %s%n", nodeException);

                    // If the error was a replica availability error, then we know that some replicas were
                    // down in this DC. Retrying in another DC could solve the problem. Other errors don't
                    // necessarily mean that the DC is unavailable, so we ignore them.
                    if (isReplicaAvailabilityError(nodeException)) {
                        failover = true;
                    }
                }
            }

            // Authorize the failover if at least one of the coordinators reported a replica availability
            // error that could be solved by trying another DC.
            if (failover) {
                System.out.println(
                        "Some nodes tried in this DC reported a replica availability error, failing over");
            } else {
                System.out.println("All nodes tried in this DC failed unexpectedly, not failing over");
            }
            return failover;

        } else if (mainException instanceof DriverTimeoutException) {

            // One or many nodes were tried, but none replied in a timely manner, and the timeout defined
            // by the option `datastax-java-driver.basic.request.timeout` was triggered.
            // This could be a DC outage as well, or a network partition issue, so trying another DC may
            // make sense.
            // Note about SLAs: if your application needs to comply with SLAs, and the maximum acceptable
            // latency for a request is equal or very close to the request timeout, beware that failing
            // over to a different datacenter here could potentially break your SLA.

            System.out.println(
                    "No node in this DC replied before the timeout was triggered, failing over");
            return true;

        } else if (mainException instanceof CoordinatorException) {

            // Only one node was tried, and it failed (and the retry policy did not tell the driver to
            // retry this request, but rather to surface the error immediately). This is rather unusual
            // as the driver's default retry policy retries most of these errors, but some custom retry
            // policies could decide otherwise. So we apply the same logic as above: if the error is a
            // replica availability error, we authorize the failover.

            Node coordinator = ((CoordinatorException) mainException).getCoordinator();
            System.out.printf(
                    "Node %s in DC %s was tried once but failed with: %s%n",
                    coordinator.getEndPoint(), coordinator.getDatacenter(), mainException);

            boolean failover = isReplicaAvailabilityError(mainException);
            if (failover) {
                System.out.println(
                        "The only node tried in this DC reported a replica availability error, failing over");
            } else {
                System.out.println("The only node tried in this DC failed unexpectedly, not failing over");
            }
            return failover;

        } else {

            // The request failed with a rather unusual error. This generally indicates a more serious
            // issue, since the retry policy decided to surface the error immediately. Trying another DC
            // is probably a bad idea.
            System.out.println("The request failed unexpectedly, not failing over: " + mainException);
            return true; // changed to true due to HeartbeatException on primary session when connections blocked
        }
    }

    /**
     * Whether the given error is a replica availability error.
     *
     * <p>A replica availability error means that the initial consistency level could not be met
     * because not enough replicas were alive.
     *
     * <p>When this error happens, it can be worth failing over to a remote DC, <em>as long as at
     * least one of the following conditions apply</em>:
     *
     * <ol>
     *   <li>if the initial consistency level was DC-local, trying another DC may succeed;
     *   <li>if the initial consistency level can be downgraded, then retrying again may succeed (in
     *       the same DC, or in another one).
     * </ol>
     * <p>
     * In this example both conditions above apply, so we authorize the failover whenever we detect a
     * replica availability error.
     */
    private boolean isReplicaAvailabilityError(Throwable t) {
        return t instanceof UnavailableException || t instanceof QueryConsistencyException;
    }

    private void close() {
        if (primarySession != null) {
            primarySession.close();
        }

        if (secondarySession != null) {
            secondarySession.close();
        }
    }
}
