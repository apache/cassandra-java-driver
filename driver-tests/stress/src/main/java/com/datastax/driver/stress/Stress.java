/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.stress;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import joptsimple.*;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple stress tool to demonstrate the use of the driver.
 * <p/>
 * Sample usage:
 * stress insert -n 100000
 * stress read -n 10000
 */
public class Stress {

    private static final Map<String, QueryGenerator.Builder> generators = new HashMap<String, QueryGenerator.Builder>();

    static {
        PropertyConfigurator.configure(System.getProperty("log4j.configuration", "./conf/log4j.properties"));

        QueryGenerator.Builder[] gs = new QueryGenerator.Builder[]{
                Generators.INSERTER,
                Generators.READER
        };

        for (QueryGenerator.Builder b : gs)
            register(b.name(), b);
    }

    private static OptionParser defaultParser() {
        OptionParser parser = new OptionParser() {{
            accepts("h", "Show this help message");
            accepts("n", "Number of requests to perform (default: unlimited)").withRequiredArg().ofType(Integer.class);
            accepts("t", "Level of concurrency to use").withRequiredArg().ofType(Integer.class).defaultsTo(50);
            accepts("async", "Make asynchronous requests instead of blocking ones");
            accepts("ip", "The hosts ip to connect to").withRequiredArg().ofType(String.class).defaultsTo("127.0.0.1");
            accepts("report-file", "The name of csv file to use for reporting results").withRequiredArg().ofType(String.class).defaultsTo("last.csv");
            accepts("print-delay", "The delay in seconds at which to report on the console").withRequiredArg().ofType(Integer.class).defaultsTo(5);
            accepts("compression", "Use compression (SNAPPY)");
            accepts("connections-per-host", "The number of connections per hosts (default: based on the number of threads)").withRequiredArg().ofType(Integer.class);
            accepts("consistency-level", "Consistency level").withRequiredArg().withValuesConvertedBy(new ConsistencyLevelConverter())
                    .ofType(ConsistencyLevel.class).defaultsTo(ConsistencyLevel.LOCAL_ONE);
        }};
        String msg = "Where <generator> can be one of " + generators.keySet() + '\n'
                + "You can get more help on a particular generator with: stress <generator> -h";
        parser.formatHelpWith(Help.formatFor("<generator>", msg));
        return parser;
    }

    public static void register(String name, QueryGenerator.Builder generator) {
        if (generators.containsKey(name))
            throw new IllegalStateException("There is already a generator registered with the name " + name);

        generators.put(name, generator);
    }

    private static class Stresser {
        private final QueryGenerator.Builder genBuilder;
        private final OptionParser parser;
        private final OptionSet options;

        private Stresser(QueryGenerator.Builder genBuilder, OptionParser parser, OptionSet options) {
            this.genBuilder = genBuilder;
            this.parser = parser;
            this.options = options;
        }

        public static Stresser forCommandLineArguments(String[] args) {
            OptionParser parser = defaultParser();

            String generatorName = findPotentialGenerator(args);
            if (generatorName == null) {
                // Still parse the options to handle -h
                OptionSet options = parseOptions(parser, args);
                System.err.println("Missing generator, you need to provide a generator.");
                printHelp(parser);
                System.exit(1);
            }

            if (!generators.containsKey(generatorName)) {
                System.err.println(String.format("Unknown generator '%s'", generatorName));
                printHelp(parser);
                System.exit(1);
            }

            QueryGenerator.Builder genBuilder = generators.get(generatorName);
            parser = genBuilder.addOptions(parser);
            OptionSet options = parseOptions(parser, args);

            List<?> nonOpts = options.nonOptionArguments();
            if (nonOpts.size() > 1) {
                System.err.println("Too many generators provided. Got " + nonOpts + " but only one generator supported.");
                printHelp(parser);
                System.exit(1);
            }

            return new Stresser(genBuilder, parser, options);
        }

        private static String findPotentialGenerator(String[] args) {
            for (String arg : args)
                if (!arg.startsWith("-"))
                    return arg;

            return null;
        }

        private static OptionSet parseOptions(OptionParser parser, String[] args) {
            try {
                OptionSet options = parser.parse(args);
                if (options.has("h")) {
                    printHelp(parser);
                    System.exit(0);
                }
                return options;
            } catch (Exception e) {
                System.err.println("Error parsing options: " + e.getMessage());
                printHelp(parser);
                System.exit(1);
                throw new AssertionError();
            }
        }

        private static void printHelp(OptionParser parser) {
            try {
                parser.printHelpOn(System.out);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        public OptionSet getOptions() {
            return options;
        }

        public void prepare(Session session) {
            genBuilder.prepare(options, session);
        }

        public QueryGenerator newGenerator(int id, Session session, int iterations) {
            return genBuilder.create(id, iterations, options, session);
        }
    }

    public static class Help implements HelpFormatter {

        private final HelpFormatter defaultFormatter;
        private final String generator;
        private final String header;

        private Help(HelpFormatter defaultFormatter, String generator, String header) {
            this.defaultFormatter = defaultFormatter;
            this.generator = generator;
            this.header = header;
        }

        public static Help formatFor(String generator, String header) {
            // It's a pain in the ass to get the real console width in JAVA so hardcode it. But it's the 21th
            // century, we're not stuck at 80 characters anymore.
            int width = 120;
            return new Help(new BuiltinHelpFormatter(width, 4), generator, header);
        }

        @Override
        public String format(Map<String, ? extends OptionDescriptor> options) {
            StringBuilder sb = new StringBuilder();

            sb.append("Usage: stress ").append(generator).append(" [<option>]*").append("\n\n");
            sb.append(header).append("\n\n");
            sb.append(defaultFormatter.format(options));
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {

        Stresser stresser = Stresser.forCommandLineArguments(args);
        OptionSet options = stresser.getOptions();

        int requests = options.has("n") ? (Integer) options.valueOf("n") : -1;
        int concurrency = (Integer) options.valueOf("t");
        ConsistencyLevel consistencyLevel = (ConsistencyLevel) options.valueOf("consistency-level");

        String reportFileName = (String) options.valueOf("report-file");

        boolean async = options.has("async");

        int iterations = (requests == -1 ? -1 : requests / concurrency);

        final int maxRequestsPerConnection = 128;
        int maxConnections = options.has("connections-per-host")
                ? (Integer) options.valueOf("connections-per-host")
                : concurrency / maxRequestsPerConnection + 1;

        PoolingOptions pools = new PoolingOptions();
        pools.setNewConnectionThreshold(HostDistance.LOCAL, concurrency);
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);

        System.out.println("Initializing stress test:");
        System.out.println("  request count:        " + (requests == -1 ? "unlimited" : requests));
        System.out.println("  concurrency:          " + concurrency + " (" + iterations + " requests/thread)");
        System.out.println("  mode:                 " + (async ? "asynchronous" : "blocking"));
        System.out.println("  per-host connections: " + maxConnections);
        System.out.println("  compression:          " + options.has("compression"));
        System.out.println("  consistency-level:    " + consistencyLevel.name());

        try {
            // Create session to hosts
            Cluster cluster = new Cluster.Builder()
                    .addContactPoints(String.valueOf(options.valueOf("ip")))
                    .withPoolingOptions(pools)
                    .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel))
                    .build();

            if (options.has("compression"))
                cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.SNAPPY);

            Session session = cluster.connect();

            Metadata metadata = cluster.getMetadata();
            System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

            System.out.println("Preparing test...");
            stresser.prepare(session);

            Reporter reporter = new Reporter((Integer) options.valueOf("print-delay"), reportFileName, args, requests);

            Consumer[] consumers = new Consumer[concurrency];
            for (int i = 0; i < concurrency; i++) {
                QueryGenerator generator = stresser.newGenerator(i, session, iterations);
                consumers[i] = async ? new AsynchronousConsumer(session, generator, reporter) :
                        new BlockingConsumer(session, generator, reporter);
            }

            System.out.println("Starting to stress test...");
            System.out.println();

            reporter.start();

            for (Consumer consumer : consumers)
                consumer.start();

            for (Consumer consumer : consumers)
                consumer.join();

            reporter.stop();

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
