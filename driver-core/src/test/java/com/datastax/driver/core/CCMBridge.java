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

import static com.datastax.driver.core.TestUtils.*;

public class CCMBridge implements CCMAccess {

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    private static final VersionNumber GLOBAL_CASSANDRA_VERSION_NUMBER;

    private static final VersionNumber GLOBAL_DSE_VERSION_NUMBER;

    private static final Set<String> CASSANDRA_INSTALL_ARGS;

    public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";

    public static final File DEFAULT_CLIENT_TRUSTSTORE_FILE = createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH);

    public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "cassandra1sfun";
    public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";

    public static final File DEFAULT_CLIENT_KEYSTORE_FILE = createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH);

    // Contain the same keypair as the client keystore, but in format usable by OpenSSL
    public static final File DEFAULT_CLIENT_PRIVATE_KEY_FILE = createTempStore("/client.key");
    public static final File DEFAULT_CLIENT_CERT_CHAIN_FILE = createTempStore("/client.crt");

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
     * A mapping of full DSE versions to their C* counterpart.  This is not meant to be comprehensive.
     * If C* version cannot be derived, the method makes a 'best guess'.
     */
    private static final Map<String, String> dseToCassandraVersions = ImmutableMap.<String, String>builder()
            .put("5.0.4", "3.0.10")
            .put("5.0.3", "3.0.9")
            .put("5.0.2", "3.0.8")
            .put("5.0.1", "3.0.7")
            .put("5.0", "3.0.7")
            .put("4.8.11", "2.1.17")
            .put("4.8.10", "2.1.15")
            .put("4.8.9", "2.1.15")
            .put("4.8.8", "2.1.14")
            .put("4.8.7", "2.1.14")
            .put("4.8.6", "2.1.13")
            .put("4.8.5", "2.1.13")
            .put("4.8.4", "2.1.12")
            .put("4.8.3", "2.1.11")
            .put("4.8.2", "2.1.11")
            .put("4.8.1", "2.1.11")
            .put("4.8", "2.1.9")
            .put("4.7.9", "2.1.15")
            .put("4.7.8", "2.1.13")
            .put("4.7.7", "2.1.12")
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
        String inputCassandraVersion = System.getProperty("cassandra.version");
        String installDirectory = System.getProperty("cassandra.directory");
        String branch = System.getProperty("cassandra.branch");

        String dseProperty = System.getProperty("dse");
        // If -Ddse, if the value is empty interpret it as enabled,
        // otherwise if there is a value, parse as boolean.
        boolean isDse = dseProperty != null && (dseProperty.isEmpty() || Boolean.parseBoolean(dseProperty));

        ImmutableSet.Builder<String> installArgs = ImmutableSet.builder();
        if (installDirectory != null && !installDirectory.trim().isEmpty()) {
            installArgs.add("--install-dir=" + new File(installDirectory).getAbsolutePath());
        } else if (branch != null && !branch.trim().isEmpty()) {
            installArgs.add("-v git:" + branch.trim().replaceAll("\"", ""));
        } else {
            installArgs.add("-v " + inputCassandraVersion);
        }

        if (isDse) {
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
            CCM_COMMAND = "powershell.exe -ExecutionPolicy Unrestricted ccm.py";
        } else {
            CCM_COMMAND = "ccm";
        }

        // If ccm.java.home is set, override the JAVA_HOME variable with it.
        String ccmJavaHome = System.getProperty("ccm.java.home");
        if (ccmJavaHome != null) {
            envMap.put("JAVA_HOME", ccmJavaHome);
        }
        ENVIRONMENT_MAP = ImmutableMap.copyOf(envMap);

        if (isDse) {
            GLOBAL_DSE_VERSION_NUMBER = VersionNumber.parse(inputCassandraVersion);
            GLOBAL_CASSANDRA_VERSION_NUMBER = CCMBridge.getCassandraVersion(GLOBAL_DSE_VERSION_NUMBER);
            logger.info("Tests requiring CCM will by default use DSE version {} (C* {}, install arguments: {})",
                    GLOBAL_DSE_VERSION_NUMBER, GLOBAL_CASSANDRA_VERSION_NUMBER, CASSANDRA_INSTALL_ARGS);
        } else {
            GLOBAL_CASSANDRA_VERSION_NUMBER = VersionNumber.parse(inputCassandraVersion);
            GLOBAL_DSE_VERSION_NUMBER = null;
            logger.info("Tests requiring CCM will by default use Cassandra version {} (install arguments: {})",
                    GLOBAL_CASSANDRA_VERSION_NUMBER, CASSANDRA_INSTALL_ARGS);
        }
    }

    /**
     * @return {@link VersionNumber} configured for Cassandra based on system properties.
     */
    public static VersionNumber getGlobalCassandraVersion() {
        return GLOBAL_CASSANDRA_VERSION_NUMBER;
    }

    /**
     * @return {@link VersionNumber} configured for DSE based on system properties.
     */
    public static VersionNumber getGlobalDSEVersion() {
        return GLOBAL_DSE_VERSION_NUMBER;
    }

    /**
     * @return The mapped cassandra version to the given dseVersion.
     * If the DSE version can't be derived the following logic is used:
     * <ol>
     * <li>If <= 3.X, use C* 1.2</li>
     * <li>If 4.X, use 2.1 for >= 4.7, 2.0 otherwise.</li>
     * <li>Otherwise 3.0</li>
     * </ol>
     */
    public static VersionNumber getCassandraVersion(VersionNumber dseVersion) {
        String cassandraVersion = dseToCassandraVersions.get(dseVersion.toString());
        if (cassandraVersion != null) {
            return VersionNumber.parse(cassandraVersion);
        } else if (dseVersion.getMajor() <= 3) {
            return VersionNumber.parse("1.2");
        } else if (dseVersion.getMajor() == 4) {
            if (dseVersion.getMinor() >= 7) {
                return VersionNumber.parse("2.1");
            } else {
                return VersionNumber.parse("2.0");
            }
        } else {
            // Fallback on 3.0 by default.
            return VersionNumber.parse("3.0");
        }
    }

    /**
     * Checks if the operating system is a Windows one
     *
     * @return <code>true</code> if the operating system is a Windows one, <code>false</code> otherwise.
     */
    public static boolean isWindows() {

        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Windows");
    }

    private final String clusterName;

    private final VersionNumber cassandraVersion;

    private final VersionNumber dseVersion;

    private final int storagePort;

    private final int thriftPort;

    private final int binaryPort;

    private final File ccmDir;

    private final boolean isDSE;

    private final String jvmArgs;

    private boolean keepLogs = false;

    private boolean started = false;

    private boolean closed = false;

    private final int[] nodes;

    private CCMBridge(String clusterName, VersionNumber cassandraVersion, VersionNumber dseVersion,
                      int storagePort, int thriftPort, int binaryPort, String jvmArgs, int[] nodes) {
        this.clusterName = clusterName;
        this.cassandraVersion = cassandraVersion;
        this.dseVersion = dseVersion;
        this.storagePort = storagePort;
        this.thriftPort = thriftPort;
        this.binaryPort = binaryPort;
        this.isDSE = dseVersion != null;
        this.jvmArgs = jvmArgs;
        this.nodes = nodes;
        this.ccmDir = Files.createTempDir();
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
    public VersionNumber getCassandraVersion() {
        return cassandraVersion;
    }

    @Override
    public VersionNumber getDSEVersion() {
        return dseVersion;
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

    /**
     * Based on C* version, return the wait arguments.
     *
     * @return For C* 1.x, --wait-other-notice otherwise --no-wait
     */
    private String getStartWaitArguments() {
        // make a small exception for C* 1.2 as it has a bug where it starts listening on the binary
        // interface slightly before it joins the cluster.
        if (this.cassandraVersion.getMajor() == 1) {
            return " --wait-other-notice";
        } else {
            return " --no-wait";
        }
    }

    @Override
    public synchronized void start() {
        if (started)
            return;
        if (logger.isDebugEnabled())
            logger.debug("Starting: {} - free memory: {} MB", this, TestUtils.getFreeMemoryMB());
        try {
            String cmd = CCM_COMMAND + " start " + jvmArgs + getStartWaitArguments();
            if (isWindows() && this.cassandraVersion.compareTo(VersionNumber.parse("2.2.4")) >= 0) {
                cmd += " --quiet-windows";
            }
            execute(cmd);

            // Wait for binary interface on each node.
            int n = 1;
            for (int dc = 1; dc <= nodes.length; dc++) {
                int nodesInDc = nodes[dc - 1];
                for (int i = 0; i < nodesInDc; i++) {
                    InetSocketAddress addr = new InetSocketAddress(ipOfNode(n), binaryPort);
                    logger.debug("Waiting for binary protocol to show up for {}", addr);
                    TestUtils.waitUntilPortIsUp(addr);
                    n++;
                }
            }
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
            String cmd = CCM_COMMAND + " node%d start " + jvmArgs + getStartWaitArguments();
            if (isWindows() && this.cassandraVersion.compareTo(VersionNumber.parse("2.2.4")) >= 0) {
                cmd += " --quiet-windows";
            }
            execute(cmd, n);
            // Wait for binary interface
            InetSocketAddress addr = new InetSocketAddress(ipOfNode(n), binaryPort);
            logger.debug("Waiting for binary protocol to show up for {}", addr);
            TestUtils.waitUntilPortIsUp(addr);
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
    public void pause(int n) {
        logger.debug(String.format("Pausing: node %s (%s%s:%s) in %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        execute(CCM_COMMAND + " node%d pause", n);
    }

    @Override
    public void resume(int n) {
        logger.debug(String.format("Resuming: node %s (%s%s:%s) in %s", n, TestUtils.IP_PREFIX, n, binaryPort, this));
        execute(CCM_COMMAND + " node%d resume", n);
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
        // Special case for C* 3.12+, DSE 5.1+, force decommission (see CASSANDRA-12510)
        String cmd = CCM_COMMAND + " node%d decommission";
        if (this.cassandraVersion.compareTo(VersionNumber.parse("3.12")) >= 0) {
            cmd += " --force";
        }
        execute(cmd, n);
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
    public void updateDSENodeConfig(int n, String key, Object value) {
        updateDSENodeConfig(n, ImmutableMap.<String, Object>builder().put(key, value).build());
    }

    @Override
    public void updateDSENodeConfig(int n, Map<String, Object> configs) {
        StringBuilder confStr = new StringBuilder();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            confStr
                    .append(entry.getKey())
                    .append(":")
                    .append(entry.getValue())
                    .append(" ");
        }
        execute(CCM_COMMAND + " node%s updatedseconf %s", n, confStr);
    }

    @Override
    public void setWorkload(int node, Workload... workload) {
        String workloadStr = Joiner.on(",").join(workload);
        execute(CCM_COMMAND + " node%d setworkload %s", node, workloadStr);
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
    @Override
    public void waitForUp(int node) {
        TestUtils.waitUntilPortIsUp(addressOfNode(node));
    }

    /**
     * Waits for a host to be down by pinging the TCP socket directly, without using the Java driver's API.
     */
    @Override
    public void waitForDown(int node) {
        TestUtils.waitUntilPortIsDown(addressOfNode(node));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        VersionNumber version = getCassandraVersion();
        if (version.compareTo(VersionNumber.parse("2.0")) < 0) {
            return ProtocolVersion.V1;
        } else if (version.compareTo(VersionNumber.parse("2.1")) < 0) {
            return ProtocolVersion.V2;
        } else if (version.compareTo(VersionNumber.parse("2.2")) < 0) {
            return ProtocolVersion.V3;
        } else {
            return ProtocolVersion.V4;
        }
    }

    @Override
    public ProtocolVersion getProtocolVersion(ProtocolVersion maximumAllowed) {
        ProtocolVersion versionToUse = getProtocolVersion();
        return versionToUse.compareTo(maximumAllowed) > 0 ? maximumAllowed : versionToUse;
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
        private boolean dse = false;
        private VersionNumber version = null;
        private Set<String> createOptions = new LinkedHashSet<String>();
        private Set<String> jvmArgs = new LinkedHashSet<String>();
        private final Map<String, Object> cassandraConfiguration = Maps.newLinkedHashMap();
        private final Map<String, Object> dseConfiguration = Maps.newLinkedHashMap();
        private Map<Integer, Workload[]> workloads = new HashMap<Integer, Workload[]>();

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
         * The Cassandra or DSE version to use.  If not specified the globally configured version is used instead.
         */
        public Builder withVersion(VersionNumber version) {
            this.version = version;
            return this;
        }

        /**
         * Indicates whether or not this cluster is meant to be a DSE cluster.
         */
        public Builder withDSE(boolean dse) {
            this.dse = dse;
            return this;
        }

        /**
         * Free-form options that will be added at the end of the {@code ccm create} command
         * (defaults to {@link #CASSANDRA_INSTALL_ARGS} if this is never called).
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
         * @param workload The workload(s) (e.g. solr, spark, hadoop)
         * @return This builder
         */
        public Builder withWorkload(int node, Workload... workload) {
            this.workloads.put(node, workload);
            return this;
        }

        public CCMBridge build() {
            // be careful NOT to alter internal state (hashCode/equals) during build!
            String clusterName = TestUtils.generateIdentifier("ccm_");


            VersionNumber dseVersion;
            VersionNumber cassandraVersion;
            boolean versionConfigured = this.version != null;
            // No version was explicitly provided, fallback on global config.
            if (!versionConfigured) {
                dseVersion = GLOBAL_DSE_VERSION_NUMBER;
                cassandraVersion = GLOBAL_CASSANDRA_VERSION_NUMBER;
            } else if (dse) {
                // given version is the DSE version, base cassandra version on DSE version.
                dseVersion = this.version;
                cassandraVersion = getCassandraVersion(dseVersion);
            } else {
                // given version is cassandra version.
                dseVersion = null;
                cassandraVersion = this.version;
            }

            Map<String, Object> cassandraConfiguration = randomizePorts(this.cassandraConfiguration);
            int storagePort = Integer.parseInt(cassandraConfiguration.get("storage_port").toString());
            int thriftPort = Integer.parseInt(cassandraConfiguration.get("rpc_port").toString());
            int binaryPort = Integer.parseInt(cassandraConfiguration.get("native_transport_port").toString());
            if (!isThriftSupported(cassandraVersion)) {
                // remove thrift configuration
                cassandraConfiguration.remove("start_rpc");
                cassandraConfiguration.remove("rpc_port");
                cassandraConfiguration.remove("thrift_prepared_statements_cache_size_mb");
            }
            final CCMBridge ccm = new CCMBridge(clusterName, cassandraVersion, dseVersion, storagePort, thriftPort, binaryPort, joinJvmArgs(), nodes);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    ccm.close();
                }
            });
            ccm.execute(buildCreateCommand(clusterName, versionConfigured, cassandraVersion, dseVersion));
            updateNodeConf(ccm);
            ccm.updateConfig(cassandraConfiguration);
            if (dseVersion != null) {
                Map<String, Object> dseConfiguration = Maps.newLinkedHashMap(this.dseConfiguration);
                if (dseVersion.getMajor() >= 5) {
                    // randomize DSE specific ports if dse present and greater than 5.0
                    dseConfiguration.put("lease_netty_server_port", RANDOM_PORT);
                    dseConfiguration.put("internode_messaging_options.port", RANDOM_PORT);
                }
                dseConfiguration = randomizePorts(dseConfiguration);
                if (!dseConfiguration.isEmpty())
                    ccm.updateDSEConfig(dseConfiguration);
            }
            for (Map.Entry<Integer, Workload[]> entry : workloads.entrySet()) {
                ccm.setWorkload(entry.getKey(), entry.getValue());
            }
            if (start)
                ccm.start();
            return ccm;
        }

        private static boolean isThriftSupported(VersionNumber cassandraVersion) {
            return cassandraVersion.compareTo(VersionNumber.parse("4.0")) < 0;
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
            String quote = isWindows() ? "\"" : "";
            for (String jvmArg : jvmArgs) {
                // Windows requires jvm arguments to be quoted, while *nix requires unquoted.
                allJvmArgs.append(" ");
                allJvmArgs.append(quote);
                allJvmArgs.append("--jvm_arg=");
                allJvmArgs.append(randomizePorts(jvmArg));
                allJvmArgs.append(quote);
            }
            return allJvmArgs.toString();
        }

        private String buildCreateCommand(String clusterName, boolean versionConfigured, VersionNumber
                cassandraVersion, VersionNumber dseVersion) {
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

            Set<String> lCreateOptions = new LinkedHashSet<String>(createOptions);
            if (!versionConfigured) {
                // If no version was provided, use the default install ags.
                lCreateOptions.addAll(CASSANDRA_INSTALL_ARGS);
            } else {
                if (dseVersion != null) {
                    lCreateOptions.add("--dse");
                    lCreateOptions.add("-v");
                    lCreateOptions.add(dseVersion.toString());
                } else {
                    lCreateOptions.add("-v");
                    lCreateOptions.add(cassandraVersion.toString());
                }
            }
            result.append(" ").append(Joiner.on(" ").join(randomizePorts(lCreateOptions)));
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
            // do not include start as it is not relevant to the settings of the cluster.
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Builder builder = (Builder) o;

            if (dse != builder.dse) return false;
            if (!Arrays.equals(nodes, builder.nodes)) return false;
            if (version != null ? !version.equals(builder.version) : builder.version != null) return false;
            if (!createOptions.equals(builder.createOptions)) return false;
            if (!jvmArgs.equals(builder.jvmArgs)) return false;
            if (!cassandraConfiguration.equals(builder.cassandraConfiguration)) return false;
            if (!dseConfiguration.equals(builder.dseConfiguration)) return false;
            return workloads.equals(builder.workloads);
        }

        @Override
        public int hashCode() {
            // do not include start as it is not relevant to the settings of the cluster.
            int result = Arrays.hashCode(nodes);
            result = 31 * result + (dse ? 1 : 0);
            result = 31 * result + (version != null ? version.hashCode() : 0);
            result = 31 * result + createOptions.hashCode();
            result = 31 * result + jvmArgs.hashCode();
            result = 31 * result + cassandraConfiguration.hashCode();
            result = 31 * result + dseConfiguration.hashCode();
            result = 31 * result + workloads.hashCode();
            return result;
        }
    }

}
