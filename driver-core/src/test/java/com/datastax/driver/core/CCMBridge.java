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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.exec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.datastax.driver.core.TestUtils.executeNoFail;
import static com.datastax.driver.core.TestUtils.findAvailablePort;

public class CCMBridge implements CCMAccess {

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    private static final String CASSANDRA_VERSION;

    private static final Set<String> CASSANDRA_INSTALL_ARGS;

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

        ImmutableSet.Builder<String> installArgs = ImmutableSet.builder();
        if (installDirectory != null && !installDirectory.trim().isEmpty()) {
            installArgs.add("--install-dir=" + new File(installDirectory).getAbsolutePath());
        } else if (branch != null && !branch.trim().isEmpty()) {
            installArgs.add("-v git:" + branch.trim().replaceAll("\"", ""));
        } else {
            installArgs.add("-v " + CASSANDRA_VERSION);
        }

        if (IS_DSE) {
            installArgs.add("--dse");
        }

        CASSANDRA_INSTALL_ARGS = installArgs.build();

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
            logger.info("Tests requiring CCM will by default use DSE version {} (C* {}, install arguments: {})",
                    CCMBridge.getDSEVersion(), CCMBridge.getCassandraVersion(), CCMBridge.getInstallArguments());
        } else {
            logger.info("Tests requiring CCM will by default use Cassandra version {} (install arguments: {})",
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

    private final VersionNumber version;

    private final int storagePort;

    private final int thriftPort;

    private final int binaryPort;

    private final File ccmDir;

    private final boolean isDSE;

    private final String jvmArgs;

    private boolean keepLogs = false;

    private boolean started = false;

    private boolean closed = false;

    private CCMBridge(String clusterName, boolean isDSE, VersionNumber version, int storagePort, int thriftPort, int binaryPort, String jvmArgs) {
        this.clusterName = clusterName;
        this.version = version;
        this.storagePort = storagePort;
        this.thriftPort = thriftPort;
        this.binaryPort = binaryPort;
        this.isDSE = isDSE;
        this.jvmArgs = jvmArgs;
        this.ccmDir = Files.createTempDir();
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
    public static Set<String> getInstallArguments() {
        return CASSANDRA_INSTALL_ARGS;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public InetSocketAddress addressOfNode(int n) {
        return new InetSocketAddress(TestUtils.ipOfNode(n), binaryPort);
    }

    @Override
    public VersionNumber getVersion() {
        return version;
    }

    @Override
    public File getCcmDir() {
        return ccmDir;
    }

    @Override
    public File getClusterDir() {
        return new File(ccmDir, clusterName);
    }

    @Override
    public File getNodeDir(int n) {
        return new File(getClusterDir(), "node" + n);
    }

    @Override
    public File getNodeConfDir(int n) {
        return new File(getNodeDir(n), "conf");
    }

    @Override
    public int getStoragePort() {
        return storagePort;
    }

    @Override
    public int getThriftPort() {
        return thriftPort;
    }

    @Override
    public int getBinaryPort() {
        return binaryPort;
    }

    @Override
    public void setKeepLogs(boolean keepLogs) {
        this.keepLogs = keepLogs;
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;
        logger.debug("Closing: {}", this);
        if (keepLogs) {
            executeNoFail(new Runnable() {
                @Override
                public void run() {
                    stop();
                }
            }, false);
            logger.info("Error during tests, kept C* logs in " + getCcmDir());
        } else {
            executeNoFail(new Runnable() {
                @Override
                public void run() {
                    remove();
                }
            }, false);
            executeNoFail(new Runnable() {
                @Override
                public void run() {
                    org.assertj.core.util.Files.delete(getCcmDir());
                }
            }, false);
        }
        closed = true;
        logger.debug("Closed: {}", this);
    }

    @Override
    public synchronized void start() {
        if (started)
            return;
        if (logger.isDebugEnabled())
            logger.debug("Starting: {} - free memory: {} MB", this, TestUtils.getFreeMemoryMB());
        try {
            execute(CCM_COMMAND + " start --wait-other-notice --wait-for-binary-proto" + jvmArgs);
        } catch (CCMException e) {
            logger.error("Could not start " + this, e);
            logger.error("CCM output:\n{}", e.getOut());
            setKeepLogs(true);
            String errors = checkForErrors();
            if (errors != null)
                logger.error("CCM check errors:\n{}", errors);
            throw e;
        }
        if (logger.isDebugEnabled())
            logger.debug("Started: {} - Free memory: {} MB", this, TestUtils.getFreeMemoryMB());
        started = true;
    }

    @Override
    public synchronized void stop() {
        if (closed)
            return;
        if (logger.isDebugEnabled())
            logger.debug("Stopping: {} - free memory: {} MB", this, TestUtils.getFreeMemoryMB());
        execute(CCM_COMMAND + " stop");
        if (logger.isDebugEnabled())
            logger.debug("Stopped: {} - free memory: {} MB", this, TestUtils.getFreeMemoryMB());
        closed = true;
    }

    @Override
    public synchronized void forceStop() {
        if (closed)
            return;
        logger.debug("Force stopping: {}", this);
        execute(CCM_COMMAND + " stop --not-gently");
        closed = true;
    }

    @Override
    public synchronized void remove() {
        stop();
        logger.debug("Removing: {}", this);
        execute(CCM_COMMAND + " remove");
    }

    @Override
    public String checkForErrors() {
        logger.debug("Checking for errors in: {}", this);
        try {
            return execute(CCM_COMMAND + " checklogerror");
        } catch (CCMException e) {
            logger.warn("Check for errors failed");
            return null;
        }
    }

    @Override
    public void start(int n) {
        logger.debug(String.format("Starting: node %s (%s%s:%s) in %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        try {
            execute(CCM_COMMAND + " node%d start --wait-other-notice --wait-for-binary-proto" + jvmArgs, n);
        } catch (CCMException e) {
            logger.error(String.format("Could not start node %s in %s", n, this), e);
            logger.error("CCM output:\n{}", e.getOut());
            setKeepLogs(true);
            String errors = checkForErrors();
            if (errors != null)
                logger.error("CCM check errors:\n{}", errors);
            throw e;
        }
    }

    @Override
    public void stop(int n) {
        logger.debug(String.format("Stopping: node %s (%s%s:%s) in %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        execute(CCM_COMMAND + " node%d stop", n);
    }

    @Override
    public void forceStop(int n) {
        logger.debug(String.format("Force stopping: node %s (%s%s:%s) in %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        execute(CCM_COMMAND + " node%d stop --not-gently", n);
    }

    @Override
    public void remove(int n) {
        logger.debug(String.format("Removing: node %s (%s%s:%s) from %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        execute(CCM_COMMAND + " node%d remove", n);
    }

    @Override
    public void add(int n) {
        add(1, n);
    }

    @Override
    public void add(int dc, int n) {
        logger.debug(String.format("Adding: node %s (%s%s:%s) to %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        String thriftItf = TestUtils.ipOfNode(n) + ":" + thriftPort;
        String storageItf = TestUtils.ipOfNode(n) + ":" + storagePort;
        String binaryItf = TestUtils.ipOfNode(n) + ":" + binaryPort;
        String remoteLogItf = TestUtils.ipOfNode(n) + ":" + TestUtils.findAvailablePort();
        execute(CCM_COMMAND + " add node%d -d dc%s -i %s%d -t %s -l %s --binary-itf %s -j %d -r %s -s -b" + (isDSE ? " --dse" : ""),
                n, dc, TestUtils.IP_PREFIX, n, thriftItf, storageItf, binaryItf, TestUtils.findAvailablePort(), remoteLogItf);
    }

    @Override
    public void decommission(int n) {
        logger.debug(String.format("Decommissioning: node %s (%s%s:%s) from %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        execute(CCM_COMMAND + " node%d decommission", n);
    }

    @Override
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

    @Override
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

    @Override
    public void updateNodeConfig(int n, String key, Object value) {
        updateNodeConfig(n, ImmutableMap.<String, Object>builder().put(key, value).build());
    }

    @Override
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

    @Override
    public void setWorkload(int node, Workload workload) {
        execute(CCM_COMMAND + " node%d setworkload %s", node, workload);
    }

    private String execute(String command, Object... args) {
        String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
        Closer closer = Closer.create();
        // 10 minutes timeout
        ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
        StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        closer.register(pw);
        try {
            logger.trace("Executing: " + fullCommand);
            CommandLine cli = CommandLine.parse(fullCommand);
            Executor executor = new DefaultExecutor();
            LogOutputStream outStream = new LogOutputStream() {
                @Override
                protected void processLine(String line, int logLevel) {
                    String out = "ccmout> " + line;
                    logger.debug(out);
                    pw.println(out);
                }
            };
            LogOutputStream errStream = new LogOutputStream() {
                @Override
                protected void processLine(String line, int logLevel) {
                    String err = "ccmerr> " + line;
                    logger.error(err);
                    pw.println(err);
                }
            };
            closer.register(outStream);
            closer.register(errStream);
            ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
            executor.setStreamHandler(streamHandler);
            executor.setWatchdog(watchDog);
            int retValue = executor.execute(cli, ENVIRONMENT_MAP);
            if (retValue != 0) {
                logger.error("Non-zero exit code ({}) returned from executing ccm command: {}", retValue, fullCommand);
                pw.flush();
                throw new CCMException(String.format("Non-zero exit code (%s) returned from executing ccm command: %s", retValue, fullCommand), sw.toString());
            }
        } catch (IOException e) {
            if (watchDog.killedProcess())
                logger.error("The command {} was killed after 10 minutes", fullCommand);
            pw.flush();
            throw new CCMException(String.format("The command %s failed to execute", fullCommand), sw.toString(), e);
        } finally {
            try {
                closer.close();
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
        return sw.toString();
    }

    /**
     * Waits for a host to be up by pinging the TCP socket directly, without using the Java driver's API.
     */
    public void waitForUp(int node) {
        TestUtils.waitUntilPortIsUp(addressOfNode(node));
    }

    /**
     * Waits for a host to be down by pinging the TCP socket directly, without using the Java driver's API.
     */
    public void waitForDown(int node) {
        TestUtils.waitUntilPortIsDown(addressOfNode(node));
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

    @Override
    public String toString() {
        return "CCM cluster " + clusterName;
    }

    @Override
    protected void finalize() throws Throwable {
        logger.debug("GC'ing {}", this);
        close();
        super.finalize();
    }

    /**
     * use {@link #builder()} to get an instance
     */
    public static class Builder {

        public static final String RANDOM_PORT = "__RANDOM_PORT__";
        private static final Pattern RANDOM_PORT_PATTERN = Pattern.compile(RANDOM_PORT);

        int[] nodes = {1};
        private boolean start = true;
        private boolean isDSE = isDSE();
        private String version = getCassandraVersion();
        private Set<String> createOptions = new LinkedHashSet<String>(getInstallArguments());
        private Set<String> jvmArgs = new LinkedHashSet<String>();
        private final Map<String, Object> cassandraConfiguration = Maps.newLinkedHashMap();
        private final Map<String, Object> dseConfiguration = Maps.newLinkedHashMap();
        private Map<Integer, Workload> workloads = new HashMap<Integer, Workload>();

        private Builder() {
            cassandraConfiguration.put("start_rpc", false);
            cassandraConfiguration.put("storage_port", RANDOM_PORT);
            cassandraConfiguration.put("rpc_port", RANDOM_PORT);
            cassandraConfiguration.put("native_transport_port", RANDOM_PORT);
        }

        /**
         * Number of hosts for each DC. Defaults to [1] (1 DC with 1 node)
         */
        public Builder withNodes(int... nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder withoutNodes() {
            return withNodes();
        }

        /**
         * Enables SSL encryption.
         */
        public Builder withSSL() {
            cassandraConfiguration.put("client_encryption_options.enabled", "true");
            cassandraConfiguration.put("client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
            cassandraConfiguration.put("client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
            return this;
        }

        /**
         * Enables client authentication.
         * This also enables encryption ({@link #withSSL()}.
         */
        public Builder withAuth() {
            withSSL();
            cassandraConfiguration.put("client_encryption_options.require_client_auth", "true");
            cassandraConfiguration.put("client_encryption_options.truststore", DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
            cassandraConfiguration.put("client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
            return this;
        }

        /**
         * Whether to start the cluster immediately (defaults to true if this is never called).
         */
        public Builder notStarted() {
            this.start = false;
            return this;
        }

        /**
         * Sets this cluster to be a DSE cluster (defaults to {@link #isDSE()} if this is never called).
         */
        public Builder withDSE() {
            this.createOptions.add("--dse");
            this.isDSE = true;
            return this;
        }

        /**
         * The Cassandra or DSE version to use (defaults to {@link #getCassandraVersion()} if this is never called).
         */
        public Builder withVersion(String version) {
            Iterator<String> it = createOptions.iterator();
            while (it.hasNext()) {
                String option = it.next();
                // remove any version previously set and
                // install-dir, which is incompatible
                if (option.startsWith("-v ") || option.startsWith("--install-dir"))
                    it.remove();
            }
            this.createOptions.add("-v " + version);
            this.version = version;
            return this;
        }

        /**
         * Free-form options that will be added at the end of the {@code ccm create} command
         * (defaults to {@link #getInstallArguments()} if this is never called).
         */
        public Builder withCreateOptions(String... createOptions) {
            Collections.addAll(this.createOptions, createOptions);
            return this;
        }

        /**
         * Customizes entries in cassandra.yaml (can be called multiple times)
         */
        public Builder withCassandraConfiguration(String key, Object value) {
            this.cassandraConfiguration.put(key, value);
            return this;
        }

        /**
         * Customizes entries in dse.yaml (can be called multiple times)
         */
        public Builder withDSEConfiguration(String key, Object value) {
            this.dseConfiguration.put(key, value);
            return this;
        }

        /**
         * JVM args to use when starting hosts.
         * System properties should be provided one by one, as a string in the form:
         * {@code -Dname=value}.
         */
        public Builder withJvmArgs(String... jvmArgs) {
            Collections.addAll(this.jvmArgs, jvmArgs);
            return this;
        }

        public Builder withStoragePort(int port) {
            cassandraConfiguration.put("storage_port", port);
            return this;
        }

        public Builder withThriftPort(int port) {
            cassandraConfiguration.put("rpc_port", port);
            return this;
        }

        public Builder withBinaryPort(int port) {
            cassandraConfiguration.put("native_transport_port", port);
            return this;
        }

        /**
         * Sets the DSE workload for a given node.
         *
         * @param node     The node to set the workload for (starting with 1).
         * @param workload The workload (e.g. solr, spark, hadoop)
         * @return This builder
         */
        public Builder withWorkload(int node, Workload workload) {
            this.workloads.put(node, workload);
            return this;
        }

        public CCMBridge build() {
            // be careful NOT to alter internal state (hashCode/equals) during build!
            String clusterName = TestUtils.generateIdentifier("ccm_");
            Map<String, Object> cassandraConfiguration = randomizePorts(this.cassandraConfiguration);
            Map<String, Object> dseConfiguration = randomizePorts(this.dseConfiguration);
            VersionNumber version = VersionNumber.parse(this.version);
            int storagePort = Integer.parseInt(cassandraConfiguration.get("storage_port").toString());
            int thriftPort = Integer.parseInt(cassandraConfiguration.get("rpc_port").toString());
            int binaryPort = Integer.parseInt(cassandraConfiguration.get("native_transport_port").toString());
            final CCMBridge ccm = new CCMBridge(clusterName, isDSE, version, storagePort, thriftPort, binaryPort, joinJvmArgs());
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    ccm.close();
                }
            });
            ccm.execute(buildCreateCommand(clusterName));
            updateNodeConf(ccm);
            ccm.updateConfig(cassandraConfiguration);
            if (!dseConfiguration.isEmpty())
                ccm.updateDSEConfig(dseConfiguration);
            for (Map.Entry<Integer, Workload> entry : workloads.entrySet()) {
                ccm.setWorkload(entry.getKey(), entry.getValue());
            }
            if (start)
                ccm.start();
            return ccm;
        }

        public int weight() {
            // the weight is simply function of the number of nodes
            int totalNodes = 0;
            for (int nodesPerDc : this.nodes) {
                totalNodes += nodesPerDc;
            }
            return totalNodes;
        }

        private String joinJvmArgs() {
            StringBuilder allJvmArgs = new StringBuilder("");
            for (String jvmArg : jvmArgs) {
                allJvmArgs.append(" --jvm_arg=");
                allJvmArgs.append(randomizePorts(jvmArg));
            }
            return allJvmArgs.toString();
        }

        private String buildCreateCommand(String clusterName) {
            StringBuilder result = new StringBuilder(CCM_COMMAND + " create");
            result.append(" ").append(clusterName);
            result.append(" -i ").append(TestUtils.IP_PREFIX);
            result.append(" ");
            if (nodes.length > 0) {
                result.append(" -n ");
                for (int i = 0; i < nodes.length; i++) {
                    int node = nodes[i];
                    if (i > 0)
                        result.append(':');
                    result.append(node);
                }
            }
            result.append(" ").append(Joiner.on(" ").join(randomizePorts(createOptions)));
            return result.toString();
        }

        /**
         * This is a workaround for an oddity in CCM:
         * when we create a cluster with -n option and
         * non-standard ports, the node.conf files are not updated accordingly.
         */
        private void updateNodeConf(CCMBridge ccm) {
            int n = 1;
            Closer closer = Closer.create();
            try {
                for (int dc = 1; dc <= nodes.length; dc++) {
                    int nodesInDc = nodes[dc - 1];
                    for (int i = 0; i < nodesInDc; i++) {
                        int jmxPort = findAvailablePort();
                        int debugPort = findAvailablePort();
                        logger.trace("Node {} in cluster {} using JMX port {} and debug port {}", n, ccm.getClusterName(), jmxPort, debugPort);
                        File nodeConf = new File(ccm.getNodeDir(n), "node.conf");
                        File nodeConf2 = new File(ccm.getNodeDir(n), "node.conf.tmp");
                        BufferedReader br = closer.register(new BufferedReader(new FileReader(nodeConf)));
                        PrintWriter pw = closer.register(new PrintWriter(new FileWriter(nodeConf2)));
                        String line;
                        while ((line = br.readLine()) != null) {
                            line = line
                                    .replace("9042", Integer.toString(ccm.binaryPort))
                                    .replace("9160", Integer.toString(ccm.thriftPort))
                                    .replace("7000", Integer.toString(ccm.storagePort));
                            if (line.startsWith("jmx_port")) {
                                line = String.format("jmx_port: '%s'", jmxPort);
                            } else if (line.startsWith("remote_debug_port")) {
                                line = String.format("remote_debug_port: %s:%s", TestUtils.ipOfNode(n), debugPort);
                            }
                            pw.println(line);
                        }
                        pw.flush();
                        pw.close();
                        Files.move(nodeConf2, nodeConf);
                        n++;
                    }
                }
            } catch (IOException e) {
                Throwables.propagate(e);
            } finally {
                try {
                    closer.close();
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
        }

        private Set<String> randomizePorts(Set<String> set) {
            Set<String> randomized = new LinkedHashSet<String>();
            for (String value : set) {
                randomized.add(randomizePorts(value));
            }
            return randomized;
        }

        private Map<String, Object> randomizePorts(Map<String, Object> map) {
            Map<String, Object> randomized = new HashMap<String, Object>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof CharSequence) {
                    value = randomizePorts((CharSequence) value);
                }
                randomized.put(entry.getKey(), value);
            }
            return randomized;
        }

        private String randomizePorts(CharSequence str) {
            Matcher matcher = RANDOM_PORT_PATTERN.matcher(str);
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(sb, Integer.toString(TestUtils.findAvailablePort()));
            }
            matcher.appendTail(sb);
            return sb.toString();
        }

        @Override
        @SuppressWarnings("SimplifiableIfStatement")
        public boolean equals(Object o) {
            // do not include cluster name and start, only
            // properties relevant to the settings of the cluster
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Builder builder = (Builder) o;
            if (isDSE != builder.isDSE) return false;
            if (!Arrays.equals(nodes, builder.nodes)) return false;
            if (!createOptions.equals(builder.createOptions)) return false;
            if (!jvmArgs.equals(builder.jvmArgs)) return false;
            if (!cassandraConfiguration.equals(builder.cassandraConfiguration)) return false;
            if (!dseConfiguration.equals(builder.dseConfiguration)) return false;
            if (!workloads.equals(builder.workloads)) return false;
            return version.equals(builder.version);
        }

        @Override
        public int hashCode() {
            // do not include cluster name and start, only
            // properties relevant to the settings of the cluster
            int result = Arrays.hashCode(nodes);
            result = 31 * result + (isDSE ? 1 : 0);
            result = 31 * result + createOptions.hashCode();
            result = 31 * result + jvmArgs.hashCode();
            result = 31 * result + cassandraConfiguration.hashCode();
            result = 31 * result + dseConfiguration.hashCode();
            result = 31 * result + workloads.hashCode();
            result = 31 * result + version.hashCode();
            return result;
        }

    }

}
