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

import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.sun.management.OperatingSystemMXBean;
import io.netty.channel.EventLoopGroup;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.ConditionChecker.check;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A number of static fields/methods handy for tests.
 */
public abstract class TestUtils {

    public static final String IP_PREFIX;

    static {
        String ip_prefix = System.getProperty("ipprefix");
        if (ip_prefix == null || ip_prefix.isEmpty()) {
            ip_prefix = "127.0.1.";
        }
        IP_PREFIX = ip_prefix;
    }

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static final String CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }";
    public static final String CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : '%s', %s }";

    public static final String CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)";

    public static final String INSERT_FORMAT = "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)";

    public static final String SELECT_ALL_FORMAT = "SELECT * FROM %s";

    public static final int TEST_BASE_NODE_WAIT = SystemProperties.getInt("com.datastax.driver.TEST_BASE_NODE_WAIT", 60);

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static BoundStatement setBoundValue(BoundStatement bs, String name, DataType type, Object value) {
        switch (type.getName()) {
            case ASCII:
                bs.setString(name, (String) value);
                break;
            case BIGINT:
                bs.setLong(name, (Long) value);
                break;
            case BLOB:
                bs.setBytes(name, (ByteBuffer) value);
                break;
            case BOOLEAN:
                bs.setBool(name, (Boolean) value);
                break;
            case COUNTER:
                // Just a no-op, we shouldn't handle counters the same way than other types
                break;
            case DECIMAL:
                bs.setDecimal(name, (BigDecimal) value);
                break;
            case DOUBLE:
                bs.setDouble(name, (Double) value);
                break;
            case FLOAT:
                bs.setFloat(name, (Float) value);
                break;
            case INET:
                bs.setInet(name, (InetAddress) value);
                break;
            case INT:
                bs.setInt(name, (Integer) value);
                break;
            case TEXT:
                bs.setString(name, (String) value);
                break;
            case TIMESTAMP:
                bs.setDate(name, (Date) value);
                break;
            case UUID:
                bs.setUUID(name, (UUID) value);
                break;
            case VARCHAR:
                bs.setString(name, (String) value);
                break;
            case VARINT:
                bs.setVarint(name, (BigInteger) value);
                break;
            case TIMEUUID:
                bs.setUUID(name, (UUID) value);
                break;
            case LIST:
                bs.setList(name, (List) value);
                break;
            case SET:
                bs.setSet(name, (Set) value);
                break;
            case MAP:
                bs.setMap(name, (Map) value);
                break;
            default:
                throw new RuntimeException("Missing handling of " + type);
        }
        return bs;
    }

    public static Object getValue(Row row, String name, DataType type) {
        switch (type.getName()) {
            case ASCII:
                return row.getString(name);
            case BIGINT:
                return row.getLong(name);
            case BLOB:
                return row.getBytes(name);
            case BOOLEAN:
                return row.getBool(name);
            case COUNTER:
                return row.getLong(name);
            case DECIMAL:
                return row.getDecimal(name);
            case DOUBLE:
                return row.getDouble(name);
            case FLOAT:
                return row.getFloat(name);
            case INET:
                return row.getInet(name);
            case INT:
                return row.getInt(name);
            case TEXT:
                return row.getString(name);
            case TIMESTAMP:
                return row.getDate(name);
            case UUID:
                return row.getUUID(name);
            case VARCHAR:
                return row.getString(name);
            case VARINT:
                return row.getVarint(name);
            case TIMEUUID:
                return row.getUUID(name);
            case LIST:
                return row.getList(name, type.getTypeArguments().get(0).asJavaClass());
            case SET:
                return row.getSet(name, type.getTypeArguments().get(0).asJavaClass());
            case MAP:
                return row.getMap(name, type.getTypeArguments().get(0).asJavaClass(), type.getTypeArguments().get(1).asJavaClass());
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Always return the "same" value for each type
    @SuppressWarnings("serial")
    public static Object getFixedValue(final DataType type) {
        try {
            switch (type.getName()) {
                case ASCII:
                    return "An ascii string";
                case BIGINT:
                    return 42L;
                case BLOB:
                    return ByteBuffer.wrap(new byte[]{(byte) 4, (byte) 12, (byte) 1});
                case BOOLEAN:
                    return true;
                case COUNTER:
                    throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
                case DECIMAL:
                    return new BigDecimal("3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679");
                case DOUBLE:
                    return 3.142519;
                case FLOAT:
                    return 3.142519f;
                case INET:
                    return InetAddress.getByAddress(new byte[]{(byte) 127, (byte) 0, (byte) 0, (byte) 1});
                case INT:
                    return 24;
                case TEXT:
                    return "A text string";
                case TIMESTAMP:
                    return new Date(1352288289L);
                case UUID:
                    return UUID.fromString("087E9967-CCDC-4A9B-9036-05930140A41B");
                case VARCHAR:
                    return "A varchar string";
                case VARINT:
                    return new BigInteger("123456789012345678901234567890");
                case TIMEUUID:
                    return UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
                case LIST:
                    return new ArrayList<Object>() {{
                        add(getFixedValue(type.getTypeArguments().get(0)));
                    }};
                case SET:
                    return new HashSet<Object>() {{
                        add(getFixedValue(type.getTypeArguments().get(0)));
                    }};
                case MAP:
                    return new HashMap<Object, Object>() {{
                        put(getFixedValue(type.getTypeArguments().get(0)), getFixedValue(type.getTypeArguments().get(1)));
                    }};
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Always return the "same" value for each type
    @SuppressWarnings("serial")
    public static Object getFixedValue2(final DataType type) {
        try {
            switch (type.getName()) {
                case ASCII:
                    return "A different ascii string";
                case BIGINT:
                    return Long.MAX_VALUE;
                case BLOB:
                    ByteBuffer bb = ByteBuffer.allocate(64);
                    bb.putInt(0xCAFE);
                    bb.putShort((short) 3);
                    bb.putShort((short) 45);
                    return bb;
                case BOOLEAN:
                    return false;
                case COUNTER:
                    throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
                case DECIMAL:
                    return new BigDecimal("12.3E+7");
                case DOUBLE:
                    return Double.POSITIVE_INFINITY;
                case FLOAT:
                    return Float.POSITIVE_INFINITY;
                case INET:
                    return InetAddress.getByName("123.123.123.123");
                case INT:
                    return Integer.MAX_VALUE;
                case TEXT:
                    return "résumé";
                case TIMESTAMP:
                    return new Date(872835240000L);
                case UUID:
                    return UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00");
                case VARCHAR:
                    return "A different varchar résumé";
                case VARINT:
                    return new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000");
                case TIMEUUID:
                    return UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
                case LIST:
                    return new ArrayList<Object>() {{
                        add(getFixedValue2(type.getTypeArguments().get(0)));
                    }};
                case SET:
                    return new HashSet<Object>() {{
                        add(getFixedValue2(type.getTypeArguments().get(0)));
                    }};
                case MAP:
                    return new HashMap<Object, Object>() {{
                        put(getFixedValue2(type.getTypeArguments().get(0)), getFixedValue2(type.getTypeArguments().get(1)));
                    }};
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Wait for a node to be up and running
    // This is used because there is some delay between when a node has been
    // added through ccm and when it's actually available for querying
    public static void waitForUp(String node, Cluster cluster) {
        waitFor(node, cluster, TEST_BASE_NODE_WAIT, false);
    }

    public static void waitForUp(String node, Cluster cluster, int timeoutSeconds) {
        waitFor(node, cluster, timeoutSeconds, false);
    }

    public static void waitForDown(String node, Cluster cluster) {
        waitFor(node, cluster, TEST_BASE_NODE_WAIT * 3, true);
    }

    public static void waitForDown(String node, Cluster cluster, int timeoutSeconds) {
        waitFor(node, cluster, timeoutSeconds, true);
    }

    private static void waitFor(String node, Cluster cluster, int timeoutSeconds, boolean waitForDown) {
        if (waitForDown)
            logger.debug("Waiting for node to leave: {}", node);
        else
            logger.debug("Waiting for upcoming node: {}", node);
        // In the case where the we've killed the last node in the cluster, if we haven't
        // tried doing an actual query, the driver won't realize that last node is dead until
        // keep alive kicks in, but that's a fairly long time. So we cheat and trigger a force
        // the detection by forcing a request.
        if (waitForDown)
            Futures.getUnchecked(cluster.manager.submitSchemaRefresh(null, null, null));
        if (waitForDown) {
            check()
                    .every(1, SECONDS)
                    .before(timeoutSeconds, SECONDS)
                    .that(new HostIsDown(cluster, node))
                    .becomesTrue();
        } else {
            check()
                    .every(1, SECONDS)
                    .before(timeoutSeconds, SECONDS)
                    .that(new HostIsUp(cluster, node))
                    .becomesTrue();
        }
    }

    private static class HostIsDown implements Callable<Boolean> {

        private final Cluster cluster;

        private final String ip;

        public HostIsDown(Cluster cluster, String ip) {
            this.cluster = cluster;
            this.ip = ip;
        }

        @Override
        public Boolean call() throws Exception {
            final Host host = findHost(cluster, ip);
            return host == null || !host.isUp();
        }
    }

    private static class HostIsUp implements Callable<Boolean> {

        private final Cluster cluster;

        private final String ip;

        public HostIsUp(Cluster cluster, String ip) {
            this.cluster = cluster;
            this.ip = ip;
        }

        @Override
        public Boolean call() throws Exception {
            final Host host = findHost(cluster, ip);
            return host != null && host.isUp();
        }
    }

    /**
     * Returns the IP of the {@code nth} host in the CCM cluster (counting from 1, i.e.,
     * {@code ipOfNode(1)} returns the IP of the first node.
     * <p/>
     * In multi-DC setups, nodes are numbered in ascending order of their datacenter number.
     * E.g. with 2 DCs and 3 nodes in each DC, the first node in DC 2 is number 4.
     *
     * @return the IP of the {@code nth} host in the cluster.
     */
    public static String ipOfNode(int n) {
        return IP_PREFIX + n;
    }

    public static Host findOrWaitForHost(Cluster cluster, int node, long duration, TimeUnit unit) {
        return findOrWaitForHost(cluster, ipOfNode(node), duration, unit);
    }

    public static Host findOrWaitForHost(final Cluster cluster, final String address, long duration, TimeUnit unit) {
        Host host = findHost(cluster, address);
        if (host == null) {
            final CountDownLatch addSignal = new CountDownLatch(1);
            Host.StateListener addListener = new StateListenerBase() {
                @Override
                public void onAdd(Host host) {
                    if (host.getAddress().getHostAddress().equals(address)) {
                        // for a new node, because of this we also listen for add events.
                        addSignal.countDown();
                    }
                }
            };
            cluster.register(addListener);
            try {
                // Wait until an add event occurs or we timeout.
                if (addSignal.await(duration, unit)) {
                    host = findHost(cluster, address);
                }
            } catch (InterruptedException e) {
                return null;
            } finally {
                cluster.unregister(addListener);
            }
        }
        return host;
    }

    public static Host findHost(Cluster cluster, int hostNumber) {
        return findHost(cluster, ipOfNode(hostNumber));
    }

    public static Host findHost(Cluster cluster, String address) {
        // Note: we can't rely on ProtocolOptions.getPort() to build an InetSocketAddress and call metadata.getHost,
        // because the port doesn't have the correct value if addContactPointsWithPorts was used to create the Cluster
        // (JAVA-860 will solve that)
        for (Host host : cluster.getMetadata().allHosts()) {
            if (host.getAddress().getHostAddress().equals(address))
                return host;
        }
        return null;
    }

    public static Host findOrWaitForControlConnection(final Cluster cluster, long duration, TimeUnit unit) {
        ControlConnection controlConnection = cluster.manager.controlConnection;
        long durationNs = TimeUnit.NANOSECONDS.convert(duration, unit);
        long start = System.nanoTime();
        while (System.nanoTime() - start < durationNs) {
            if (controlConnection.isOpen()) {
                return controlConnection.connectedHost();
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
        return null;
    }

    public static HostConnectionPool poolOf(Session session, int hostNumber) {
        SessionManager sessionManager = (SessionManager) session;
        return sessionManager.pools.get(findHost(session.getCluster(), hostNumber));
    }

    public static int numberOfLocalCoreConnections(Cluster cluster) {
        Configuration configuration = cluster.getConfiguration();
        return configuration.getPoolingOptions().getCoreConnectionsPerHost(HostDistance.LOCAL);
    }

    /**
     * @return A Scassandra instance with an arbitrarily chosen binary port from 8042-8142 and admin port from
     * 8052-8152.
     */
    public static Scassandra createScassandraServer() {
        int binaryPort = findAvailablePort();
        int adminPort = findAvailablePort();
        return ScassandraFactory.createServer(ipOfNode(1), binaryPort, ipOfNode(1), adminPort);
    }

    private static final ConcurrentMap<String, AtomicInteger> IDENTIFIERS = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * Generates a unique CQL identifier with the given prefix.
     *
     * @param prefix The prefix for the identifier
     * @return a unique CQL identifier.
     */
    public static String generateIdentifier(String prefix) {
        AtomicInteger seq = new AtomicInteger(0);
        AtomicInteger previous = IDENTIFIERS.putIfAbsent(prefix, seq);
        if (previous != null)
            seq = previous;
        return prefix + seq.incrementAndGet();
    }

    /**
     * Finds an available port in the ephemeral range.
     * This is loosely inspired by Apache MINA's AvailablePortFinder.
     *
     * @return A local port that is currently unused.
     */
    public synchronized static int findAvailablePort() throws RuntimeException {
        ServerSocket ss = null;
        try {
            // let the system pick an ephemeral port
            ss = new ServerSocket(0);
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
        }
    }

    private static final Predicate<InetSocketAddress> PORT_IS_UP = new Predicate<InetSocketAddress>() {

        @Override
        public boolean apply(InetSocketAddress address) {
            return pingPort(address.getAddress(), address.getPort());
        }

    };

    public static void waitUntilPortIsUp(InetSocketAddress address) {
        check().before(5, MINUTES).that(address, PORT_IS_UP).becomesTrue();
    }

    public static void waitUntilPortIsDown(InetSocketAddress address) {
        check().before(5, MINUTES).that(address, PORT_IS_UP).becomesFalse();
    }

    public static boolean pingPort(InetAddress address, int port) {
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

    /**
     * @return The desired target protocol version based on the 'cassandra.version' System property.
     */
    public static ProtocolVersion getDesiredProtocolVersion() {
        String version = System.getProperty("cassandra.version");
        String[] versionArray = version.split("\\.|-");
        double major = Double.parseDouble(versionArray[0] + "." + versionArray[1]);
        if (major < 2.0) {
            return ProtocolVersion.V1;
        } else if (major < 2.1) {
            return ProtocolVersion.V2;
        } else {
            return ProtocolVersion.V3;
        }
    }

    /**
     * @return a {@link Cluster} instance that connects only to the control host of the given cluster.
     */
    public static Cluster buildControlCluster(Cluster cluster, CCMAccess ccm) {
        Host controlHost = cluster.manager.controlConnection.connectedHost();
        List<InetSocketAddress> singleAddress = Collections.singletonList(controlHost.getSocketAddress());
        return Cluster.builder()
                .addContactPointsWithPorts(singleAddress)
                .withPort(ccm.getBinaryPort())
                .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), singleAddress))
                .build();
    }

    /**
     * @return a {@link QueryOptions} that disables debouncing by setting intervals to 0ms.
     */
    public static QueryOptions nonDebouncingQueryOptions() {
        return new QueryOptions().setRefreshNodeIntervalMillis(0)
                .setRefreshNodeListIntervalMillis(0)
                .setRefreshSchemaIntervalMillis(0);
    }

    /**
     * A custom {@link NettyOptions} that shuts down the {@link EventLoopGroup} after
     * no quiet time.  This is useful for tests that consistently close clusters as
     * otherwise there is a 2 second delay (from JAVA-914).
     */
    public static NettyOptions nonQuietClusterCloseOptions = new NettyOptions() {
        @Override
        public void onClusterClose(EventLoopGroup eventLoopGroup) {
            eventLoopGroup.shutdownGracefully(0, 15, SECONDS).syncUninterruptibly();
        }
    };

    /**
     * Executes a task and catches any exception.
     *
     * @param task         The task to execute.
     * @param logException Whether to log the exception, if any.
     */
    public static void executeNoFail(Runnable task, boolean logException) {
        try {
            task.run();
        } catch (Exception e) {
            if (logException)
                logger.error(e.getMessage(), e);
        }
    }

    /**
     * Executes a task and catches any exception.
     *
     * @param task         The task to execute.
     * @param logException Whether to log the exception, if any.
     */
    public static void executeNoFail(Callable<?> task, boolean logException) {
        try {
            task.call();
        } catch (Exception e) {
            if (logException)
                logger.error(e.getMessage(), e);
        }
    }

    /**
     * Returns the system's free memory in megabytes.
     * <p/>
     * This includes the free physical memory + the free swap memory.
     */
    public static long getFreeMemoryMB() {
        OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return (bean.getFreePhysicalMemorySize() + bean.getFreeSwapSpaceSize()) / 1024 / 1024;
    }

}
