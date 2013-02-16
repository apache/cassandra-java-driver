/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.stress;

import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * A simple stress tool to demonstrate the use of the driver.
 *
 * Sample usage:
 *   stress insert -n 100000
 *   stress read -n 10000
 */
public class Stress {

    private static final Map<String, QueryGenerator.Builder> generators = new HashMap<String, QueryGenerator.Builder>();

    public static void register(String name, QueryGenerator.Builder generator) {
        if (generators.containsKey(name))
            throw new IllegalStateException("There is already a generator registered with the name " + name);

        generators.put(name, generator);
    }

    private static void printHelp(OptionParser parser, Collection<String> generators) throws Exception {

        System.out.println("Usage: stress <generator> [<option>]*\n");
        System.out.println("Where <generator> can be one of " + generators);
        System.out.println();
        parser.printHelpOn(System.out);
    }

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();

        parser.accepts("?", "Show this help message");
        parser.accepts("n", "Number of iterations for the query generator").withRequiredArg().ofType(Integer.class).defaultsTo(1000000);
        parser.accepts("t", "Number of threads to use").withRequiredArg().ofType(Integer.class).defaultsTo(50);
        parser.accepts("csv", "Save metrics into csv instead of displaying on stdout");
        parser.accepts("columns-per-row", "Number of columns per CQL3 row").withRequiredArg().ofType(Integer.class).defaultsTo(5);
        parser.accepts("value-size", "The size in bytes for column values").withRequiredArg().ofType(Integer.class).defaultsTo(34);
        parser.accepts("ip", "The hosts ip to connect to").withRequiredArg().ofType(String.class).defaultsTo("127.0.0.1");

        register("insert", Generators.CASSANDRA_INSERTER);
        register("insert_prepared", Generators.CASSANDRA_PREPARED_INSERTER);

        if (args.length < 1) {
            System.err.println("Missing argument, you must at least provide the action to do");
            printHelp(parser, generators.keySet());
            System.exit(1);
        }

        String action = args[0];
        if (!generators.containsKey(action)) {
            System.err.println(String.format("Unknown generator '%s'", action));
            printHelp(parser, generators.keySet());
            System.exit(1);
        }

        String[] opts = new String[args.length - 1];
        System.arraycopy(args, 1, opts, 0, opts.length);

        OptionSet options = null;
        try {
            options = parser.parse(opts);
        } catch (Exception e) {
            System.err.println("Error parsing options: " + e.getMessage());
            printHelp(parser, generators.keySet());
            System.exit(1);
        }

        int iterations = (Integer)options.valueOf("n");
        int threads = (Integer)options.valueOf("t");

        QueryGenerator generator = generators.get(action).create(iterations, options);

        boolean async = false;
        boolean useCsv = options.has("csv");

        BlockingQueue<QueryGenerator.Request> workQueue = new SynchronousQueue<QueryGenerator.Request>(true);

        try {
            // Create session to hosts
            Cluster cluster = new Cluster.Builder().addContactPoints(String.valueOf(options.valueOf("ip"))).build();

            //PoolingOptions pools = cluster.getConfiguration().getConnectionsConfiguration().getPoolingOptions();
            //pools.setCoreConnectionsPerHost(HostDistance.LOCAL, 2);
            //pools.setMaxConnectionsPerHost(HostDistance.LOCAL, 2);

            Session session = cluster.connect();

            Metadata metadata = cluster.getMetadata();
            System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

            System.out.println("Creating schema...");
            generator.createSchema(session);

            Reporter reporter = new Reporter(useCsv);
            Producer producer = new Producer(generator, workQueue);

            Consumer[] consumers = new Consumer[threads];
            Consumer.Asynchronous.ResultHandler resultHandler = async ? new Consumer.Asynchronous.ResultHandler() : null;
            for (int i = 0; i < threads; i++) {
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
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
