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

import java.io.IOException;
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

    private static final OptionParser parser = new OptionParser() {{
        accepts("h", "Show this help message");
        accepts("n", "Number of requests to perform (default: unlimited)").withRequiredArg().ofType(Integer.class);
        accepts("t", "Level of concurrency to use").withRequiredArg().ofType(Integer.class).defaultsTo(50);
        accepts("async", "Make asynchronous requests instead of blocking ones");
        accepts("csv", "Save metrics into csv instead of displaying on stdout");
        accepts("columns-per-row", "Number of columns per CQL3 row").withRequiredArg().ofType(Integer.class).defaultsTo(5);
        accepts("value-size", "The size in bytes for column values").withRequiredArg().ofType(Integer.class).defaultsTo(34);
        accepts("ip", "The hosts ip to connect to").withRequiredArg().ofType(String.class).defaultsTo("127.0.0.1");
    }};

    public static void register(String name, QueryGenerator.Builder generator) {
        if (generators.containsKey(name))
            throw new IllegalStateException("There is already a generator registered with the name " + name);

        generators.put(name, generator);
    }

    private static void printHelp(OptionParser parser, Collection<String> generators) {

        System.out.println("Usage: stress <generator> [<option>]*\n");
        System.out.println("Where <generator> can be one of " + generators);
        System.out.println();

        try {
            parser.printHelpOn(System.out);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static OptionSet parseOptions(String[] args) {
        try {
            OptionSet options = parser.parse(args);
            if (options.has("h")) {
                printHelp(parser, generators.keySet());
                System.exit(0);
            }
            return options;
        } catch (Exception e) {
            System.err.println("Error parsing options: " + e.getMessage());
            printHelp(parser, generators.keySet());
            System.exit(1);
            throw new AssertionError();
        }
    }

    private static QueryGenerator.Builder getGenerator(OptionSet options) {
        register("insert", Generators.CASSANDRA_INSERTER);
        register("insert_prepared", Generators.CASSANDRA_PREPARED_INSERTER);

        List<?> args = options.nonOptionArguments();
        if (args.isEmpty()) {
            System.err.println("Missing generator, you need to provide a generator.");
            printHelp(parser, generators.keySet());
            System.exit(1);
        }

        if (args.size() > 1) {
            System.err.println("Too many generators provided. Got " + args + " but only one generator supported.");
            printHelp(parser, generators.keySet());
            System.exit(1);
        }

        String action = (String)args.get(0);
        if (!generators.containsKey(action)) {
            System.err.println(String.format("Unknown generator '%s'", action));
            printHelp(parser, generators.keySet());
            System.exit(1);
        }

        return generators.get(action);
    }

    public static void main(String[] args) throws Exception {

        OptionSet options = parseOptions(args);
        QueryGenerator.Builder genBuilder = getGenerator(options);

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
            pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, concurrency);
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
