package com.datastax.driver.core;

import java.io.*;
import java.net.InetAddress;
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

    private static final Logger logger = Logger.getLogger(CCMBridge.class);

    public static final String IP_PREFIX;

    private static final String CASSANDRA_VERSION_REGEXP = "\\d\\.\\d\\.\\d(-\\w+)?";

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

        String ip_prefix = System.getProperty("ipprefix");
        if (ip_prefix == null || ip_prefix.equals("")) {
            ip_prefix = "127.0.1.";
        }
        IP_PREFIX = ip_prefix;
    }

    private final Runtime runtime = Runtime.getRuntime();
    private final File ccmDir;

    private CCMBridge()
    {
        this.ccmDir = Files.createTempDir();
    }

    public static CCMBridge create(String name) {
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -b -i %s %s", name, IP_PREFIX, CASSANDRA_VERSION);
        return bridge;
    }

    public static CCMBridge create(String name, int nbNodes) {
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -n %d -s -i %s -b %s", name, nbNodes, IP_PREFIX, CASSANDRA_VERSION);
        return bridge;
    }

    public static CCMBridge create(String name, int nbNodesDC1, int nbNodesDC2) {
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -n %d:%d -s -i %s -b %s", name, nbNodesDC1, nbNodesDC2, IP_PREFIX, CASSANDRA_VERSION);
        return bridge;
    }

    public static CCMBridge.CCMCluster buildCluster(int nbNodes, Cluster.Builder builder) {
        return CCMCluster.create(nbNodes, builder);
    }

    public static CCMBridge.CCMCluster buildCluster(int nbNodesDC1, int nbNodesDC2, Cluster.Builder builder) {
        return CCMCluster.create(nbNodesDC1, nbNodesDC2, builder);
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
        execute("ccm add node%d -i %s%d -j %d -b", n, IP_PREFIX, n, 7000 + 100*n);
        execute("ccm node%d start", n);
    }

    private void execute(String command, Object... args) {
        try {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
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

    // One cluster for the whole test class
    public static abstract class PerClassSingleNodeCluster {

        protected static CCMBridge cassandraCluster;
        private static boolean erroredOut;
        private static boolean schemaCreated;

        protected static Cluster cluster;
        protected static Session session;

        protected abstract Collection<String> getTableDefinitions();

        public void errorOut() {
            erroredOut = true;
        }

        @BeforeClass
        public static void createCluster() {
            erroredOut = false;
            schemaCreated = false;
            cassandraCluster = CCMBridge.create("test", 1);
            try {
            	cluster = Cluster.builder().addContactPoints(IP_PREFIX + "1").build();
                session = cluster.connect();
            } catch (NoHostAvailableException e) {
                erroredOut = true;
                for (Map.Entry<InetAddress, String> entry : e.getErrors().entrySet())
                    logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
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

    public static class CCMCluster {

        public final Cluster cluster;
        public final Session session;

        public final CCMBridge bridge;

        private boolean erroredOut;

        public static CCMCluster create(int nbNodes, Cluster.Builder builder) {
            if (nbNodes == 0)
                throw new IllegalArgumentException();

            return new CCMCluster(CCMBridge.create("test", nbNodes), builder);
        }

        public static CCMCluster create(int nbNodesDC1, int nbNodesDC2, Cluster.Builder builder) {
            if (nbNodesDC1 == 0)
                throw new IllegalArgumentException();

            return new CCMCluster(CCMBridge.create("test", nbNodesDC1, nbNodesDC2), builder);
        }

        private CCMCluster(CCMBridge bridge, Cluster.Builder builder) {
            this.bridge = bridge;
            try {
                this.cluster = builder.addContactPoints(IP_PREFIX + "1").build();
                this.session = cluster.connect();

            } catch (NoHostAvailableException e) {
                for (Map.Entry<InetAddress, String> entry : e.getErrors().entrySet())
                    logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
                throw new RuntimeException(e);
            }
        }

        public void errorOut() {
            erroredOut = true;
        }

        public void discard() {
            if (cluster != null)
                cluster.shutdown();

            if (bridge == null) {
                logger.error("No cluster to discard");
            } else if (erroredOut) {
                bridge.stop();
                logger.info("Error during tests, kept C* logs in " + bridge.ccmDir);
            } else {
                bridge.remove();
                bridge.ccmDir.delete();
            }
        }
    }
}
