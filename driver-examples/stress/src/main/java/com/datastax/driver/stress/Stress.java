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
        parser.accepts("n", "Number of requests to perform (default: unlimited)").withRequiredArg().ofType(Integer.class);
        parser.accepts("t", "Level of concurrency to use").withRequiredArg().ofType(Integer.class).defaultsTo(50);
        parser.accepts("async", "Make asynchronous requests instead of blocking ones");
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

        QueryGenerator.Builder genBuilder = generators.get(action);

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

        int requests = options.has("n") ? (Integer)options.valueOf("n") : -1;
        int concurrency = (Integer)options.valueOf("t");

        boolean async = options.has("async");
        boolean useCsv = options.has("csv");

        System.out.println("Initializing stress test...");
        System.out.println("request count: " + (requests == -1 ? "unlimited" : requests));
        System.out.println("concurrency: " + concurrency);
        System.out.println("mode: " + (async ? "asynchronous" : "blocking"));

        try {
            // Create session to hosts
            Cluster cluster = new Cluster.Builder().addContactPoints(String.valueOf(options.valueOf("ip"))).build();

            final int maxRequestsPerConnection = 128;
            int maxConnections = concurrency / maxRequestsPerConnection + 1;

            PoolingOptions pools = cluster.getConfiguration().getPoolingOptions();
            pools.setMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.LOCAL, concurrency);
            pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
            pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
            pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
            pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);

            Session session = cluster.connect();

            Metadata metadata = cluster.getMetadata();
            System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

            System.out.println("Creating schema...");
            genBuilder.createSchema(options, session);

            Reporter reporter = new Reporter(useCsv);

            Consumer[] consumers = new Consumer[concurrency];
            for (int i = 0; i < concurrency; i++) {
                int iterations = (requests  == -1 ? -1 : requests / concurrency);
                QueryGenerator generator = genBuilder.create(i, iterations, options, session);
                consumers[i] = async ? new AsynchronousConsumer(session, generator, reporter) :
                                       new BlockingConsumer(session, generator, reporter);
            }

            System.out.println("Starting to stress test...");

            for (Consumer consumer : consumers)
                consumer.start();

            for (Consumer consumer : consumers)
                consumer.join();

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
