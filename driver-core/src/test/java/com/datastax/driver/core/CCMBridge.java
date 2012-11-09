package com.datastax.driver.core;

import java.io.*;
import java.util.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.exceptions.*;
import static com.datastax.driver.core.TestUtils.*;

import com.google.common.io.Files;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class CCMBridge {

    static {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-5p [%t]: %m%n")));
    }

    private static final Logger logger = Logger.getLogger(CCMBridge.class);

    private static final String CASSANDRA_VERSION_REGEXP = "cassandra-\\d\\.\\d\\.\\d(-\\w+)?";

    private static final File CASSANDRA_DIR;
    private static final String CASSANDRA_VERSION;
    static {
        String version = System.getProperty("cassandra.version");
        if (version.matches(CASSANDRA_VERSION_REGEXP)) {
            CASSANDRA_DIR = null;
            CASSANDRA_VERSION = "-v " + version;
        } else {
            CASSANDRA_DIR = new File(version);
            CASSANDRA_VERSION = "";
        }
    }

    private final Runtime runtime = Runtime.getRuntime();
    private final File ccmDir;

    private CCMBridge()
    {
        this.ccmDir = Files.createTempDir();
    }

    public static CCMBridge create(String name) {
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -b %s", name, CASSANDRA_VERSION);
        // Small sleep, otherwise the cluster is not always available because ccm create don't wait for the client server to be up
        //try { Thread.sleep(500); } catch (InterruptedException e) {}
        return bridge;
    }

    public static CCMBridge create(String name, int nbNodes) {
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -n %d -s -b %s", name, nbNodes, CASSANDRA_VERSION);
        // See above
        //try { Thread.sleep(500); } catch (InterruptedException e) {}
        return bridge;
    }

    public void start() {
        execute("ccm start");
    }

    public void stop() {
        execute("ccm stop");
    }

    public void start(int n) {
        execute("ccm node%d start", n);
    }

    public void stop(int n) {
        execute("ccm node%d stop", n);
    }

    public void remove() {
        stop();
        execute("ccm remove");
    }

    public void bootstrapNode(int n) {
        execute("ccm add node%d -i 127.0.0.%d -s; ccm start", n, n);
    }

    private void execute(String command, Object... args) {

        try {
            Process p = runtime.exec(String.format(command, args) + " --config-dir=" + ccmDir, null, CASSANDRA_DIR);
            int retValue = p.waitFor();

            if (retValue != 0) {
                BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                String line = outReader.readLine();
                while (line != null) {
                    logger.info("out> " + line);
                    line = outReader.readLine();
                }
                line = errReader.readLine();
                while (line != null) {
                    logger.error("err> " + line);
                    line = errReader.readLine();
                }
                throw new RuntimeException();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static abstract class PerClassSingleNodeCluster {

        protected static CCMBridge cassandraCluster;
        private static boolean erroredOut;
        private static boolean schemaCreated;

        protected static Cluster cluster;
        protected static Session session;

        protected abstract Collection<String> getTableDefinitions();

        @BeforeClass
        public static void createCluster() {
            erroredOut = false;
            schemaCreated = false;
            cassandraCluster = CCMBridge.create("test", 1);
            try {
                cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build();
                session = cluster.connect();
            } catch (NoHostAvailableException e) {
                erroredOut = true;
                throw new RuntimeException(e);
            }
        }

        @AfterClass
        public static void discardCluster() {
            if (cluster != null)
                cluster.shutdown();

            if (cassandraCluster == null) {
                logger.error("No cluster to discard");
            } else if (erroredOut) {
                cassandraCluster.stop();
                logger.info("Error during tests, kept C* logs in " + cassandraCluster.ccmDir);
            } else {
                cassandraCluster.remove();
                cassandraCluster.ccmDir.delete();
            }
        }

        @Before
        public void maybeCreateSchema() throws NoHostAvailableException {

            try {
                if (schemaCreated)
                    return;

                try {
                    session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, SIMPLE_KEYSPACE, 1));
                } catch (AlreadyExistsException e) {
                    // It's ok, ignore
                }

                session.execute("USE " + SIMPLE_KEYSPACE);

                for (String tableDef : getTableDefinitions()) {
                    try {
                        session.execute(tableDef);
                    } catch (AlreadyExistsException e) {
                        // It's ok, ignore
                    }
                }

                schemaCreated = true;
            } catch (NoHostAvailableException e) {
                erroredOut = true;
                throw e;
            }
        }
    }
}
