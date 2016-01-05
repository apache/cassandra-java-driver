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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.exec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.fail;

@SuppressWarnings("unused")
public class CCMBridge {

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    public static final String IP_PREFIX;

    private static final String CASSANDRA_VERSION;

    private static final String CASSANDRA_INSTALL_ARGS;

    private static final boolean IS_DSE;

    public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";

    public static final File DEFAULT_CLIENT_TRUSTSTORE_FILE = createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH);

    public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";

    public static final File DEFAULT_CLIENT_KEYSTORE_FILE = createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH);

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
     * <p/>
     * At times it is necessary to use a separate java install for CCM then what is being used for running tests.
     * For example, if you want to run tests with JDK 6 but against Cassandra 2.0, which requires JDK 7.
     */
    private static final Map<String, String> ENVIRONMENT_MAP;

    /**
     * A mapping of full DSE versions to their C* counterpart.  This is not meant to be comprehensive.  Used by
     * {@link #getCassandraVersion()}.  If C* version cannot be derived, the method makes a 'best guess'.
     */
    private static final Map<String, String> dseToCassandraVersions = ImmutableMap.<String, String>builder()
            .put("5.0", "3.0")
            .put("4.8.3", "2.1.11")
            .put("4.8.2", "2.1.11")
            .put("4.8.1", "2.1.11")
            .put("4.8", "2.1.9")
            .put("4.7.6", "2.1.11")
            .put("4.7.5", "2.1.11")
            .put("4.7.4", "2.1.11")
            .put("4.7.3", "2.1.8")
            .put("4.7.2", "2.1.8")
            .put("4.7.1", "2.1.5")
            .put("4.6.11", "2.0.16")
            .put("4.6.10", "2.0.16")
            .put("4.6.9", "2.0.16")
            .put("4.6.8", "2.0.16")
            .put("4.6.7", "2.0.14")
            .put("4.6.6", "2.0.14")
            .put("4.6.5", "2.0.14")
            .put("4.6.4", "2.0.14")
            .put("4.6.3", "2.0.12")
            .put("4.6.2", "2.0.12")
            .put("4.6.1", "2.0.12")
            .put("4.6", "2.0.11")
            .put("4.5.9", "2.0.16")
            .put("4.5.8", "2.0.14")
            .put("4.5.7", "2.0.12")
            .put("4.5.6", "2.0.12")
            .put("4.5.5", "2.0.12")
            .put("4.5.4", "2.0.11")
            .put("4.5.3", "2.0.11")
            .put("4.5.2", "2.0.10")
            .put("4.5.1", "2.0.8")
            .put("4.5", "2.0.8")
            .put("4.0", "2.0")
            .put("3.2", "1.2")
            .put("3.1", "1.2")
            .build();

    /**
     * The command to use to launch CCM
     */
    private static final String CCM_COMMAND;

    static {
        CASSANDRA_VERSION = System.getProperty("cassandra.version");
        String installDirectory = System.getProperty("cassandra.directory");
        String branch = System.getProperty("cassandra.branch");

        String dseProperty = System.getProperty("dse");
        // If -Ddse, if the value is empty interpret it as enabled,
        // otherwise if there is a value, parse as boolean.
        IS_DSE = dseProperty != null && (dseProperty.isEmpty() || Boolean.parseBoolean(dseProperty));

        StringBuilder installArgs = new StringBuilder();
        if (installDirectory != null && !installDirectory.trim().isEmpty()) {
            installArgs.append("--install-dir=");
            installArgs.append(new File(installDirectory).getAbsolutePath());
        } else if (branch != null && !branch.trim().isEmpty()) {
            installArgs.append("-v git:");
            installArgs.append(branch.trim().replaceAll("\"", ""));
        } else {
            installArgs.append("-v ");
            installArgs.append(CASSANDRA_VERSION);
        }

        if (IS_DSE) {
            installArgs.append(" --dse");
        }

        CASSANDRA_INSTALL_ARGS = installArgs.toString();

        String ip_prefix = System.getProperty("ipprefix");
        if (ip_prefix == null || ip_prefix.isEmpty()) {
            ip_prefix = "127.0.1.";
        }
        IP_PREFIX = ip_prefix;

        // Inherit the current environment.
        Map<String, String> envMap = Maps.newHashMap(new ProcessBuilder().environment());
        // If ccm.path is set, override the PATH variable with it.
        String ccmPath = System.getProperty("ccm.path");
        if (ccmPath != null) {
            String existingPath = envMap.get("PATH");
            if (existingPath == null) {
                existingPath = "";
            }
            envMap.put("PATH", ccmPath + File.pathSeparator + existingPath);
        }

        if (isWindows()) {
            CCM_COMMAND = "cmd /c ccm.py";
        } else {
            CCM_COMMAND = "ccm";
        }

        // If ccm.java.home is set, override the JAVA_HOME variable with it.
        String ccmJavaHome = System.getProperty("ccm.java.home");
        if (ccmJavaHome != null) {
            envMap.put("JAVA_HOME", ccmJavaHome);
        }
        ENVIRONMENT_MAP = ImmutableMap.copyOf(envMap);

        if (CCMBridge.isDSE()) {
            logger.info("Tests requiring CCM will use DSE version {} (C* {}, install arguments: {})",
                    CCMBridge.getDSEVersion(), CCMBridge.getCassandraVersion(), CCMBridge.getInstallArguments());
        } else {
            logger.info("Tests requiring CCM will use Cassandra version {} (install arguments: {})",
                    CCMBridge.getCassandraVersion(), CCMBridge.getInstallArguments());
        }
    }

    /**
     * Checks if the operating system is a Windows one
     *
     * @return <code>true</code> if the operating system is a Windows one, <code>false</code> otherwise.
     */
    private static boolean isWindows() {

        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Windows");
    }

    private final String clusterName;

    private final File ccmDir;

    private final boolean isDSE;

    private final String jvmArgs;

    private CCMBridge(String clusterName, boolean isDSE, String... jvmArgs) {
        this.clusterName = clusterName;
        this.ccmDir = Files.createTempDir();
        this.isDSE = isDSE;
        this.jvmArgs = joinJvmArgs(jvmArgs);
    }

    /**
     * @return The configured cassandra version.  If -Ddse=true was used, this value is derived from the
     * DSE version provided.  If the DSE version can't be derived the following logic is used:
     * <ol>
     * <li>If <= 3.X, use C* 1.2</li>
     * <li>If 4.X, use 2.1 for >= 4.7, 2.0 otherwise.</li>
     * <li>Otherwise 3.0</li>
     * </ol>
     */
    public static String getCassandraVersion() {
        if (isDSE()) {
            String cassandraVersion = dseToCassandraVersions.get(CASSANDRA_VERSION);
            if (cassandraVersion != null) {
                return cassandraVersion;
            } else if (CASSANDRA_VERSION.startsWith("3.") || CASSANDRA_VERSION.compareTo("3") <= 0) {
                return "1.2";
            } else if (CASSANDRA_VERSION.startsWith("4.")) {
                if (CASSANDRA_VERSION.compareTo("4.7") >= 0) {
                    return "2.1";
                } else {
                    return "2.0";
                }
            } else {
                // Fallback on 3.0 by default.
                return "3.0";
            }

        } else {
            return CASSANDRA_VERSION;
        }
    }

    /**
     * @return The configured DSE version if '-Ddse=true' specified, otherwise null.
     */
    public static String getDSEVersion() {
        if (isDSE()) {
            return CASSANDRA_VERSION;
        } else {
            return null;
        }
    }

    /**
     * @return Whether or not DSE was configured via '-Ddse=true'.
     */
    public static boolean isDSE() {
        return IS_DSE;
    }

    /**
     * @return The install arguments to pass to CCM when creating the cluster.
     */
    public static String getInstallArguments() {
        return CASSANDRA_INSTALL_ARGS;
    }

    public File getCcmDir() {
        return ccmDir;
    }

    public File getClusterDir() {
        return new File(ccmDir, clusterName);
    }

    public File getNodeDir(int n) {
        return new File(getClusterDir(), "node" + n);
    }

    public File getNodeConfDir(int n) {
        return new File(getNodeDir(n), "conf");
    }

    /**
     * <p>
     * Extracts a keystore from the classpath into a temporary file.
     * </p>
     * <p/>
     * <p>
     * This is needed as the keystore could be part of a built test jar used by other
     * projects, and they need to be extracted to a file system so cassandra may use them.
     * </p>
     *
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

    public static Builder builder() {
        return new Builder();
    }

    public static CCMBridge.CCMCluster buildCluster(int nbNodes, Cluster.Builder builder) {
        return CCMCluster.create(nbNodes, builder);
    }

    public static CCMBridge.CCMCluster buildCluster(int nbNodesDC1, int nbNodesDC2, Cluster.Builder builder) {
        return CCMCluster.create(nbNodesDC1, nbNodesDC2, builder);
    }

    public void start() {
        logger.info("Starting: " + this);
        execute(CCM_COMMAND + " start --wait-other-notice --wait-for-binary-proto" + jvmArgs);
    }

    public void stop() {
        logger.info("Stopping: " + this);
        execute(CCM_COMMAND + " stop");
    }

    public void forceStop() {
        logger.info("Force stopping: " + this);
        execute(CCM_COMMAND + " stop --not-gently");
    }

    public void start(int n) {
        logger.info(String.format("Starting: node %s (%s%s) in %s", n, IP_PREFIX, n, this) +
                (jvmArgs.isEmpty() ? "" : " with JVM args:" + jvmArgs));
        execute(CCM_COMMAND + " node%d start --wait-other-notice --wait-for-binary-proto" + jvmArgs, n);
    }

    public void stop(int n) {
        logger.info(String.format("Stopping: node %s (%s%s) in %s", n, IP_PREFIX, n, this));
        execute(CCM_COMMAND + " node%d stop", n);
    }

    public void forceStop(int n) {
        logger.info(String.format("Force stopping: node %s (%s.%s) in %s", n, IP_PREFIX, n, this));
        execute(CCM_COMMAND + " node%d stop --not-gently", n);
    }

    public void remove() {
        stop();
        logger.info("Removing: " + this);
        execute(CCM_COMMAND + " remove");
    }

    public void remove(int n) {
        logger.info(String.format("Removing: node %s (%s%s) from %s", n, IP_PREFIX, n, this));
        execute(CCM_COMMAND + " node%d remove", n);
    }

    public void bootstrapNode(int n) {
        bootstrapNode(n, null);
    }

    public void bootstrapNode(int n, String dc) {
        if (dc == null)
            execute(CCM_COMMAND + " add node%d -i %s%d -j %d -r %d -b -s" + (isDSE ? " --dse" : ""), n, IP_PREFIX, n, 7000 + 100 * n, 8000 + 100 * n);
        else
            execute(CCM_COMMAND + " add node%d -i %s%d -j %d -b -d %s -s" + (isDSE ? " --dse" : ""), n, IP_PREFIX, n, 7000 + 100 * n, dc);
        execute(CCM_COMMAND + " node%d start --wait-other-notice --wait-for-binary-proto", n);
    }

    public void addNode(int dc, int n, long initialToken, int storagePort, int binaryPort, int jmxPort, int remoteDebugPort) {
        String storageItf = IP_PREFIX + n + ":" + storagePort;
        String binaryItf = IP_PREFIX + n + ":" + binaryPort;
        String remoteLogItf = IP_PREFIX + n + ":" + remoteDebugPort;
        execute(CCM_COMMAND + " add node%d -d dc%s -i %s%d -n %s -l %s --binary-itf %s -j %d -r %s -s" + (isDSE ? " --dse" : ""),
                n, dc, IP_PREFIX, n, initialToken, storageItf, binaryItf, jmxPort, remoteLogItf);
    }

    public void decommissionNode(int n) {
        logger.info(String.format("Decommissioning: node %s (%s%s) from %s", n, IP_PREFIX, n, this));
        execute(CCM_COMMAND + " node%d decommission", n);
    }

    public void updateConfig(Map<String, Object> configs) {
        StringBuilder confStr = new StringBuilder();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            confStr
                    .append(entry.getKey())
                    .append(":")
                    .append(entry.getValue())
                    .append(" ");
        }
        execute(CCM_COMMAND + " updateconf " + confStr);
    }

    public void updateDSEConfig(Map<String, Object> configs) {
        StringBuilder confStr = new StringBuilder();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            confStr
                    .append(entry.getKey())
                    .append(":")
                    .append(entry.getValue())
                    .append(" ");
        }
        execute(CCM_COMMAND + " updatedseconf " + confStr);
    }

    public void updateNodeConfig(int n, String key, Object value) {
        updateNodeConfig(n, ImmutableMap.<String, Object>builder().put(key, value).build());
    }

    public void updateNodeConfig(int n, Map<String, Object> configs) {
        StringBuilder confStr = new StringBuilder();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            confStr
                    .append(entry.getKey())
                    .append(":")
                    .append(entry.getValue())
                    .append(" ");
        }
        execute(CCM_COMMAND + " node%s updateconf %s", n, confStr);
    }

    public void setWorkload(int node, String workload) {
        execute(CCM_COMMAND + " node%d setworkload %s", node, workload);
    }

    private void execute(String command, Object... args) {

        String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
        try {
            logger.debug("Executing: " + fullCommand);
            CommandLine cli = CommandLine.parse(fullCommand);
            Executor executor = new DefaultExecutor();

            LogOutputStream outStream = new LogOutputStream() {
                @Override
                protected void processLine(String line, int logLevel) {
                    logger.debug("ccmout> " + line);
                }
            };
            LogOutputStream errStream = new LogOutputStream() {
                @Override
                protected void processLine(String line, int logLevel) {
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
            throw new RuntimeException(String.format("The command %s failed to execute", fullCommand), e);
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

    private static String joinJvmArgs(String... jvmArgs) {
        StringBuilder allJvmArgs = new StringBuilder("");
        if (jvmArgs != null) {
            for (String jvmArg : jvmArgs) {
                allJvmArgs.append(" --jvm_arg=");
                allJvmArgs.append(jvmArg);
            }
        }
        return allJvmArgs.toString();
    }

    @Override
    public String toString() {
        return String.format("CCM cluster %s [%s]", clusterName, ccmDir);
    }

    public static class CCMCluster {

        public final Cluster cluster;
        public final Session session;

        public final CCMBridge cassandraCluster;

        private boolean erroredOut;

        public static CCMCluster create(int nbNodes, Cluster.Builder builder) {
            if (nbNodes == 0)
                throw new IllegalArgumentException();

            CCMBridge ccm = CCMBridge.builder().withNodes(nbNodes).build();
            return new CCMCluster(ccm, builder, nbNodes);
        }

        public static CCMCluster create(int nbNodesDC1, int nbNodesDC2, Cluster.Builder builder) {
            if (nbNodesDC1 == 0)
                throw new IllegalArgumentException();

            CCMBridge ccm = CCMBridge.builder().withNodes(nbNodesDC1, nbNodesDC2).build();
            return new CCMCluster(ccm, builder, nbNodesDC1 + nbNodesDC2);
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
                } catch (InterruptedException e) {
                    fail("Unexpected interruption");
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
                if (!cassandraCluster.ccmDir.delete())
                    logger.warn("Could not delete CCM data dir: " + cassandraCluster.ccmDir);
            }
        }
    }

    /**
     * use {@link #builder()} to get an instance
     */
    public static class Builder {
        private static final AtomicInteger COUNTER = new AtomicInteger(0);
        private final String clusterName = "test_" + COUNTER.incrementAndGet();
        private int[] nodes = {1};
        private boolean start = true;
        private boolean isDSE = IS_DSE;
        private String cassandraInstallArgs = CASSANDRA_INSTALL_ARGS;
        private String[] startOptions = new String[0];
        private String[] jvmArgs;
        private Map<String, Object> cassandraConfiguration = Maps.newHashMap();
        private Map<String, Object> dseConfiguration = Maps.newHashMap();

        /**
         * Number of nodes for each DC. Defaults to [1] (1 DC with 1 node)
         */
        public Builder withNodes(int... nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder withoutNodes() {
            return withNodes();
        }

        public Builder withSSL(boolean requireClientAuth) {
            cassandraConfiguration.put("client_encryption_options.enabled", "true");
            cassandraConfiguration.put("client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
            cassandraConfiguration.put("client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);

            if (requireClientAuth) {
                cassandraConfiguration.put("client_encryption_options.require_client_auth", "true");
                cassandraConfiguration.put("client_encryption_options.truststore", DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
                cassandraConfiguration.put("client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
            }
            return this;
        }

        /**
         * Whether to start the cluster immediately (defaults to true if this is never called)
         */
        public Builder notStarted() {
            this.start = false;
            return this;
        }

        /**
         * Defaults to system property cassandra.version
         */
        public Builder withCassandraVersion(String cassandraVersion) {
            this.cassandraInstallArgs = "-v " + cassandraVersion;
            this.isDSE = false;
            return this;
        }

        public Builder withDSEVersion(String dseVersion) {
            this.cassandraInstallArgs = "-v " + dseVersion + " --dse";
            this.isDSE = true;
            return this;
        }

        /**
         * Free-form options that will be added at the end of the start command
         */
        public Builder withStartOptions(String... options) {
            this.startOptions = options;
            return this;
        }

        /**
         * Customizes entries in cassandra.yaml (can be called multiple times)
         */
        public Builder withCassandraConfiguration(String key, Object value) {
            this.cassandraConfiguration.put(key, value);
            return this;
        }

        public Builder withDSEConfiguration(String key, Object value) {
            this.dseConfiguration.put(key, value);
            return this;
        }

        /**
         * JVM args to use when starting nodes.
         * System properties should be provided in the form:
         * {@code -Dname=value}.
         */
        public Builder withJvmArgs(String... jvmArgs) {
            this.jvmArgs = jvmArgs;
            return this;
        }

        public CCMBridge build() {
            CCMBridge ccm = new CCMBridge(clusterName, isDSE, jvmArgs);
            ccm.execute(buildCreateCommand());
            if (!cassandraConfiguration.isEmpty())
                ccm.updateConfig(cassandraConfiguration);
            if (!dseConfiguration.isEmpty())
                ccm.updateDSEConfig(dseConfiguration);
            if (start)
                ccm.start();
            return ccm;
        }

        private String buildCreateCommand() {
            StringBuilder result = new StringBuilder(CCM_COMMAND + " create");
            result.append(" ").append(clusterName);
            result.append(" -i ").append(IP_PREFIX);
            result.append(" ").append(cassandraInstallArgs);
            if (nodes.length > 0) {
                result.append(" -n ");
                for (int i = 0; i < nodes.length; i++) {
                    if (i > 0) result.append(":");
                    result.append(nodes[i]);
                }
            }
            if (startOptions.length > 0)
                result.append(" ").append(Joiner.on(" ").join(startOptions));
            return result.toString();
        }

        @Override
        @SuppressWarnings("SimplifiableIfStatement")
        public boolean equals(Object o) {
            // do not include cluster name
            if (this == o) return true;
            if (!(o instanceof Builder)) return false;
            Builder builder = (Builder) o;
            if (start != builder.start) return false;
            if (isDSE != builder.isDSE) return false;
            if (!Arrays.equals(nodes, builder.nodes)) return false;
            if (!cassandraInstallArgs.equals(builder.cassandraInstallArgs)) return false;
            if (!Arrays.equals(startOptions, builder.startOptions)) return false;
            if (!Arrays.equals(jvmArgs, builder.jvmArgs)) return false;
            if (!cassandraConfiguration.equals(builder.cassandraConfiguration)) return false;
            return dseConfiguration.equals(builder.dseConfiguration);
        }

        @Override
        public int hashCode() {
            // do not include cluster name
            int result = Arrays.hashCode(nodes);
            result = 31 * result + (start ? 1 : 0);
            result = 31 * result + (isDSE ? 1 : 0);
            result = 31 * result + cassandraInstallArgs.hashCode();
            result = 31 * result + Arrays.hashCode(startOptions);
            result = 31 * result + Arrays.hashCode(jvmArgs);
            result = 31 * result + cassandraConfiguration.hashCode();
            result = 31 * result + dseConfiguration.hashCode();
            return result;
        }

    }

}
