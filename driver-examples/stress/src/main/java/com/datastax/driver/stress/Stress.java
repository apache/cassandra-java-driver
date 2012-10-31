package com.datastax.driver.examples.stress;

import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.configuration.*;
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

    public static void main(String[] args) throws Exception {

        register("insert", Generators.SIMPLE_INSERTER);
        register("insert_prepared", Generators.SIMPLE_PREPARED_INSERTER);

        if (args.length < 2) {
            System.err.println("Missing argument, you must at least provide the action to do");
            System.exit(1);
        }

        String action = args[1];
        if (!generators.containsKey(action)) {
            System.err.println(String.format("Unknown generator '%s' (known generators: %s)", action, generators.keySet()));
            System.exit(1);
        }

        String[] opts = new String[args.length - 2];
        System.arraycopy(args, 2, opts, 0, opts.length);

        OptionParser parser = new OptionParser();

        parser.accepts("n", "Number of iterations for the query generator (default: 1,000,000)").withRequiredArg().ofType(Integer.class);
        parser.accepts("t", "Number of threads to use (default: 30)").withRequiredArg().ofType(Integer.class);

        OptionSet options = parser.parse(opts);

        int ITERATIONS = options.has("n") ? (Integer)options.valueOf("n") : 1000000;
        int THREADS = options.has("t") ? (Integer)options.valueOf("t") : 30;

        QueryGenerator generator = generators.get(action).create(ITERATIONS);

        boolean async = false;

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
