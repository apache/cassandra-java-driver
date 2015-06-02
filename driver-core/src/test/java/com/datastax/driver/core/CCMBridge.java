/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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
package com.datastax.driver.core;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.exec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.fail;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.TestUtils.SIMPLE_KEYSPACE;

public class CCMBridge {

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    public static final String IP_PREFIX;

    private static final String CASSANDRA_VERSION_REGEXP = "\\d\\.\\d\\.\\d+(-\\w+)?";

    public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";

    public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";

    public static final String DEFAULT_SERVER_TRUSTSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_SERVER_TRUSTSTORE_PATH = "/server.truststore";

    private static final File DEFAULT_SERVER_TRUSTSTORE_FILE = createTempStore(DEFAULT_SERVER_TRUSTSTORE_PATH);

    public static final String DEFAULT_SERVER_KEYSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_SERVER_KEYSTORE_PATH = "/server.keystore";

    private static final File DEFAULT_SERVER_KEYSTORE_FILE = createTempStore(DEFAULT_SERVER_KEYSTORE_PATH);

    /**
     * The environment variables to use when invoking CCM.  Inherits the current processes environment, but will also
     * prepend to the PATH variable the value of the 'ccm.path' property and set JAVA_HOME variable to the
     * 'ccm.java.home' variable.
     *
     * At times it is necessary to use a separate java install for CCM then what is being used for running tests.
     * For example, if you want to run tests with JDK 6 but against Cassandra 2.0, which requires JDK 7.
     */
    private static final Map<String,String> ENVIRONMENT_MAP;

    static {
        // Inherit the current environment.
        Map<String,String> envMap = Maps.newHashMap(new ProcessBuilder().environment());
        // If ccm.path is set, override the PATH variable with it.
        String ccmPath = System.getProperty("ccm.path");
        if(ccmPath != null) {
            String existingPath = envMap.get("PATH");
            if(existingPath == null) {
                existingPath = "";
            }
            envMap.put("PATH", ccmPath + ":" + existingPath);
        }
        // If ccm.java.home is set, override the JAVA_HOME variable with it.
        String ccmJavaHome = System.getProperty("ccm.java.home");
        if(ccmJavaHome != null) {
            envMap.put("JAVA_HOME", ccmJavaHome);
        }
        ENVIRONMENT_MAP = ImmutableMap.copyOf(envMap);
    }

    static final File CASSANDRA_DIR;
    static final String CASSANDRA_VERSION;

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
        if (ip_prefix == null || ip_prefix.isEmpty()) {
            ip_prefix = "127.0.1.";
        }
        IP_PREFIX = ip_prefix;
    }

    private final Runtime runtime = Runtime.getRuntime();
    private final File ccmDir;

    private CCMBridge() {
        this.ccmDir = Files.createTempDir();
    }

    /**
     * <p>
     * Extracts a keystore from the classpath into a temporary file.
     * </p>
     *
     * <p>
     * This is needed as the keystore could be part of a built test jar used by other
     * projects, and they need to be extracted to a file system so cassandra may use them.
     * </p>
     * @param storePath Path in classpath where the keystore exists.
     * @return The generated File.
     */
    private static File createTempStore(String storePath) {
        File f = null;
        Closer closer = Closer.create();
        try {
            InputStream trustStoreIs = CCMBridge.class.getResourceAsStream(storePath);
            closer.register(trustStoreIs);
            f = File.createTempFile("server", ".store");
            logger.debug("Created store file {} for {}.", f, storePath);
            OutputStream trustStoreOs = new FileOutputStream(f);
            closer.register(trustStoreOs);
            ByteStreams.copy(trustStoreIs, trustStoreOs);
        } catch (IOException e) {
            logger.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e);
        } finally {
            try {
                closer.close();
            } catch (IOException e) {
                logger.warn("Failure closing streams.", e);
            }
        }
        return f;
    }

    public static CCMBridge create(String name, String... options) {
        // This leads to a confusing CCM error message so check explicitly:
        checkArgument(!"current".equals(name.toLowerCase()),
            "cluster can't be called \"current\"");
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -b -i %s %s " + Joiner.on(" ").join(options), name, IP_PREFIX, CASSANDRA_VERSION);
        return bridge;
    }

    public static CCMBridge create(String name, int nbNodes, String... options) {
        checkArgument(!"current".equals(name.toLowerCase()),
            "cluster can't be called \"current\"");
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -n %d -s -i %s -b %s " + Joiner.on(" ").join(options), name, nbNodes, IP_PREFIX, CASSANDRA_VERSION);
        return bridge;
    }

    public static CCMBridge createWithCustomVersion(String name, int nbNodes, String cassandraVersion) {
        checkArgument(!"current".equals(name.toLowerCase()),
            "cluster can't be called \"current\"");
        CCMBridge bridge = new CCMBridge();
        bridge.execute("ccm create %s -n %d -s -i %s -b -v %s ", name, nbNodes, IP_PREFIX, cassandraVersion);
        return bridge;
    }

    public static CCMBridge create(String name, int nbNodesDC1, int nbNodesDC2) {
        checkArgument(!"current".equals(name.toLowerCase()),
            "cluster can't be called \"current\"");
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
        execute("ccm start --wait-other-notice --wait-for-binary-proto");
    }

    public void stop() {
        execute("ccm stop");
    }

    public void forceStop() {
        execute("ccm stop --not-gently");
    }

    public void start(int n) {
        logger.info("Starting: " + IP_PREFIX + n);
        execute("ccm node%d start --wait-other-notice --wait-for-binary-proto", n);
    }

    public void start(int n, String option) {
        logger.info("Starting: " + IP_PREFIX + n + " with " + option);
        execute("ccm node%d start --wait-other-notice --wait-for-binary-proto --jvm_arg=%s", n, option);
    }

    public void stop(int n) {
        logger.info("Stopping: " + IP_PREFIX + n);
        execute("ccm node%d stop", n);
    }

    public void stop(String clusterName) {
        logger.info("Stopping Cluster : "+clusterName);
        execute("ccm stop "+clusterName);
    }

    public void forceStop(int n) {
        logger.info("Force stopping: " + IP_PREFIX + n);
        execute("ccm node%d stop --not-gently", n);
    }

    public void remove() {
        stop();
        execute("ccm remove");
    }

    public void remove(String clusterName) {
        stop(clusterName);
        execute("ccm remove " + clusterName);
    }

    public void remove(int n) {
        logger.info("Removing: " + IP_PREFIX + n);
        execute("ccm node%d remove", n);
    }

    public void bootstrapNode(int n) {
        bootstrapNode(n, null);
    }

    public void bootstrapNode(int n, String dc) {
        if (dc == null)
            execute("ccm add node%d -i %s%d -j %d -r %d -b -s", n, IP_PREFIX, n, 7000 + 100 * n, 8000 + 100 * n);
        else
            execute("ccm add node%d -i %s%d -j %d -b -d %s -s", n, IP_PREFIX, n, 7000 + 100 * n, dc);
        execute("ccm node%d start --wait-other-notice --wait-for-binary-proto", n);
    }

    public void bootstrapNodeWithPorts(int n, int thriftPort, int storagePort, int binaryPort, int jmxPort, int remoteDebugPort) {
        String thriftItf = IP_PREFIX + n + ":" + thriftPort;
        String storageItf = IP_PREFIX + n + ":" + storagePort;
        String binaryItf = IP_PREFIX + n + ":" + binaryPort;
        String remoteLogItf = IP_PREFIX + n + ":" + remoteDebugPort;
        execute("ccm add node%d -i %s%d -b -t %s -l %s --binary-itf %s -j %d -r %s -s",
            n, IP_PREFIX, n, thriftItf, storageItf, binaryItf, jmxPort, remoteLogItf);
        execute("ccm node%d start --wait-other-notice --wait-for-binary-proto", n);
    }

    public void decommissionNode(int n) {
        execute("ccm node%d decommission", n);
    }

    public void updateConfig(String name, String value) {
        updateConfig(ImmutableMap.of(name, value));
    }

    public void updateConfig(Map<String, String> configs) {
        StringBuilder confStr = new StringBuilder();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            confStr.append(entry.getKey() + ":" + entry.getValue() + " ");
        }
        execute("ccm updateconf " + confStr);
    }

    public void populate(int n) {
        execute("ccm populate -n %d -i %s", n, IP_PREFIX);
    }

    private void execute(String command, Object... args) {
        try {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            CommandLine cli = CommandLine.parse(fullCommand);
            Executor executor = new DefaultExecutor();

            LogOutputStream outStream = new LogOutputStream() {
                @Override protected void processLine(String line, int logLevel) {
                    logger.debug("ccmout> " + line);
                }
            };
            LogOutputStream errStream = new LogOutputStream() {
                @Override protected void processLine(String line, int logLevel) {
                    logger.error("ccmerr> " + line);
                }
            };

            ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
            executor.setStreamHandler(streamHandler);

            int retValue = executor.execute(cli, ENVIRONMENT_MAP);
            if (retValue != 0) {
                logger.error("Non-zero exit code ({}) returned from executing ccm command: {}", retValue, fullCommand);
                throw new RuntimeException();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits for a host to be up by pinging the TCP socket directly, without using the Java driver's API.
     */
    public void waitForUp(int node) {
        try {
            InetAddress address = InetAddress.getByName(ipOfNode(node));
            CCMBridge.busyWaitForPort(address, 9042, true);
        } catch (UnknownHostException e) {
            fail("Unknown host " + ipOfNode(node) + "( node " + node + " of CCMBridge)");
        }
    }

    /**
     * Waits for a host to be down by pinging the TCP socket directly, without using the Java driver's API.
     */
    public void waitForDown(int node) {
        try {
            InetAddress address = InetAddress.getByName(ipOfNode(node));
            CCMBridge.busyWaitForPort(address, 9042, false);
        } catch (UnknownHostException e) {
            fail("Unknown host " + ipOfNode(node) + "( node " + node + " of CCMBridge)");
        }
    }

    /**
     * Enables SSL without client authentication.  Does not apply to already started nodes.
     */
    public void enableSSL() {
        enableSSL(false);
    }

    /**
     * Enables SSL.  Does not apply to already started nodes.
     * @param requireClientAuth Whether or not to require Clients used authentication.
     */
    public void enableSSL(boolean requireClientAuth) {
        ImmutableMap.Builder<String, String> configs = ImmutableMap.builder();

        configs.put("client_encryption_options.enabled", "true");
        configs.put("client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
        configs.put("client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);

        if (requireClientAuth) {
            configs.put("client_encryption_options.require_client_auth", "true");
            configs.put("client_encryption_options.truststore", DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
            configs.put("client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
        }

        updateConfig(configs.build());
    }

    private static void busyWaitForPort(InetAddress address, int port, boolean expectedConnectionState) {
        long maxAcceptableWaitTime = TimeUnit.SECONDS.toMillis(10);
        long waitQuantum = TimeUnit.MILLISECONDS.toMillis(500);
        long waitTimeSoFar = 0;
        boolean connectionState = !expectedConnectionState;

        while (connectionState != expectedConnectionState && waitTimeSoFar < maxAcceptableWaitTime) {
            connectionState = CCMBridge.pingPort(address, port);
            try {
                Thread.sleep(waitQuantum);
                waitTimeSoFar += waitQuantum;
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while pinging " + address + ":" + port, e);
            }
        }
    }

    private static boolean pingPort(InetAddress address, int port) {
        logger.debug("Trying {}:{}...", address, port);
        boolean connectionSuccessful = false;
        Socket socket = null;
        try {
            socket = new Socket(address, port);
            connectionSuccessful = true;
            logger.debug("Successfully connected");
        } catch (IOException e) {
            logger.debug("Connection failed");
        } finally {
            if (socket != null)
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.warn("Error closing socket to " + address);
                }
        }
        return connectionSuccessful;
    }

    public static String ipOfNode(int nodeNumber) {
        return IP_PREFIX + Integer.toString(nodeNumber);
    }

    public static class terminationHook extends Thread {
        public void run() {
            logger.debug("shut down hook task..");

            if (PerClassSingleNodeCluster.cluster != null) {
                PerClassSingleNodeCluster.cluster.close();
            }
            if (PerClassSingleNodeCluster.ccmBridge == null) {
                logger.error("No cluster to discard");
            } else if (PerClassSingleNodeCluster.erroredOut) {
                PerClassSingleNodeCluster.ccmBridge.remove("test-class");
                logger.info("Error during tests, kept C* logs in " + PerClassSingleNodeCluster.ccmBridge.ccmDir);
            } else {
                PerClassSingleNodeCluster.ccmBridge.remove("test-class");
                PerClassSingleNodeCluster.ccmBridge.ccmDir.delete();
            }

        }
    }

    // One cluster for the whole test class
    public static abstract class PerClassSingleNodeCluster {

        protected static CCMBridge ccmBridge;
        private static boolean erroredOut;
        private static boolean clusterInitialized=false;
        private static AtomicLong ksNumber;
        protected String keyspace;

        protected static InetSocketAddress hostAddress;
        protected static int[] ports;

        protected static Cluster cluster;
        protected static Session session;
        
        protected final VersionNumber cassandraVersion = VersionNumber.parse(System.getProperty("cassandra.version"));

        protected abstract Collection<String> getTableDefinitions();

        // Give individual tests a chance to customize the cluster configuration
        protected Cluster.Builder configure(Cluster.Builder builder) {
            return builder;
        }

        public void errorOut() {
            erroredOut = true;
        }

        @BeforeClass(groups = { "short", "long" })
        public void beforeClass() {
            maybeInitCluster();
            initKeyspace();
        }

        @AfterClass(groups = { "short", "long" })
        public void afterClass() {
            clearSimpleKeyspace();
        }

        private void maybeInitCluster(){
            if (!clusterInitialized){
                try {
                    //launch ccm cluster
                    ccmBridge = CCMBridge.create("test-class");

                    ports = new int[5];
                    for (int i = 0; i < 5; i++) {
                        ports[i] = TestUtils.findAvailablePort(11000 + i);
                    }

                    ccmBridge.bootstrapNodeWithPorts(1, ports[0], ports[1], ports[2], ports[3], ports[4]);
                    ksNumber = new AtomicLong(0);
                    erroredOut = false;
                    hostAddress = new InetSocketAddress(InetAddress.getByName(IP_PREFIX + 1), ports[2]);

                    Runtime r = Runtime.getRuntime();
                    r.addShutdownHook(new terminationHook());
                    clusterInitialized = true;

                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }

        }


        private void initKeyspace() {
            try {
                Builder builder = Cluster.builder();

                builder = configure(builder);

                cluster = builder.addContactPointsWithPorts(Collections.singletonList(hostAddress)).build();
                session = cluster.connect();
                keyspace = SIMPLE_KEYSPACE + "_" + ksNumber.incrementAndGet();
                session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));

                session.execute("USE " + keyspace);
                for (String tableDef : getTableDefinitions()) {
                    try {
                        session.execute(tableDef);
                    } catch (AlreadyExistsException e) {
                        // It's ok, ignore
                    }
                }
            } catch (AlreadyExistsException e) {
                // It's ok, ignore (not supposed to go there)
            } catch (NoHostAvailableException e) {
                erroredOut = true;
                for (Map.Entry<InetSocketAddress, Throwable> entry : e.getErrors().entrySet())
                    logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
                throw new RuntimeException(e);
            } catch (DriverException e) {
                erroredOut = true;
                throw e;
            }
        }

        private void clearSimpleKeyspace() {
            session.execute("DROP KEYSPACE " + keyspace);
            if (cluster != null) {
                cluster.close();
            }
        }

    }

    public static class CCMCluster {

        public final Cluster cluster;
        public final Session session;

        public final CCMBridge cassandraCluster;

        private boolean erroredOut;

        public static CCMCluster create(int nbNodes, Cluster.Builder builder) {
            if (nbNodes == 0)
                throw new IllegalArgumentException();

            return new CCMCluster(CCMBridge.create("test", nbNodes), builder, nbNodes);
        }

        public static CCMCluster create(int nbNodesDC1, int nbNodesDC2, Cluster.Builder builder) {
            if (nbNodesDC1 == 0)
                throw new IllegalArgumentException();

            return new CCMCluster(CCMBridge.create("test", nbNodesDC1, nbNodesDC2), builder, nbNodesDC1 + nbNodesDC2);
        }

        public static CCMCluster create(CCMBridge cassandraCluster, Cluster.Builder builder, int totalNodes) {
            return new CCMCluster(cassandraCluster, builder, totalNodes);
        }

        private CCMCluster(CCMBridge cassandraCluster, Cluster.Builder builder, int totalNodes) {
            this.cassandraCluster = cassandraCluster;
            try {
                String[] contactPoints = new String[totalNodes];
                for (int i = 0; i < totalNodes; i++)
                    contactPoints[i] = IP_PREFIX + (i + 1);

                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
                this.cluster = builder.addContactPoints(contactPoints).build();
                this.session = cluster.connect();
            } catch (NoHostAvailableException e) {
                for (Map.Entry<InetSocketAddress, Throwable> entry : e.getErrors().entrySet())
                    logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
                discard();
                throw new RuntimeException(e);
            }
        }

        public void errorOut() {
            erroredOut = true;
        }

        public void discard() {
            if (cluster != null)
                cluster.close();

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
    }
}
