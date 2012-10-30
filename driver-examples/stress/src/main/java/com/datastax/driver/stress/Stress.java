package com.datastax.driver.examples.stress;

import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.configuration.*;
import com.datastax.driver.core.exceptions.*;

/**
 * A simple stress tool to demonstrate the use of the driver.
 *
 * Sample usage:
 *   stress insert -n 100000
 *   stress read -n 10000
 */
public class Stress {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Missing arguments");
            System.exit(1);
        }

        final int ITERATIONS = Integer.parseInt(args[1]);
        final int THREADS = Integer.parseInt(args[2]);

        boolean async = false;

        QueryGenerator generator = new QueryGenerator() {

            private int i;

            public void createSchema(Session session) throws NoHostAvailableException {
                try { session.execute("CREATE KEYSPACE stress_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"); } catch (AlreadyExistsException e) { /* It's ok, ignore */ }
                session.execute("USE stress_ks");

                try {
                    session.execute("CREATE TABLE stress_cf (k int, c int, v int, PRIMARY KEY (k, c))");
                } catch (AlreadyExistsException e) { /* It's ok, ignore */ }
            }

            public boolean hasNext() {
                return i < ITERATIONS;
            }

            public QueryGenerator.Request next() {
                String query = String.format("INSERT INTO stress_cf(k, c, v) VALUES (%d, %d, %d)", i, i, i);
                ++i;
                return new QueryGenerator.Request.SimpleQuery(query, new QueryOptions());
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        BlockingQueue<QueryGenerator.Request> workQueue = new SynchronousQueue<QueryGenerator.Request>(true);

        try {
            // Create session to hosts
            Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build();

            //PoolingOptions pools = cluster.getConfiguration().getConnectionsConfiguration().getPoolingOptions();
            //pools.setCoreConnectionsPerHost(HostDistance.LOCAL, 2);
            //pools.setMaxConnectionsPerHost(HostDistance.LOCAL, 2);

            Session session = cluster.connect();

            ClusterMetadata metadata = cluster.getMetadata();
            System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

            System.out.println("Creating schema...");
            generator.createSchema(session);

            Reporter reporter = new Reporter();
            Producer producer = new Producer(generator, workQueue);

            Consumer[] consumers = new Consumer[THREADS];
            Consumer.Asynchronous.ResultHandler resultHandler = async ? new Consumer.Asynchronous.ResultHandler() : null;
            for (int i = 0; i < THREADS; i++) {
                consumers[i] = async
                             ? new Consumer.Asynchronous(session, workQueue, reporter, resultHandler)
                             : new Consumer(session, workQueue, reporter);
            }

            System.out.println("Starting to stress test...");
            producer.start();
            if (resultHandler != null)
                resultHandler.start();
            for (Consumer consumer : consumers)
                consumer.start();

            producer.join();
            for (Consumer consumer : consumers)
                consumer.join();
            if (resultHandler != null)
                resultHandler.join();

            System.out.println("Stress test successful.");
            System.exit(0);

        } catch (NoHostAvailableException e) {
            System.err.println("No alive hosts to use: " + e.getMessage());
            System.exit(1);
        } catch (QueryExecutionException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (QueryValidationException e) {
            System.err.println("Invalid query: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
