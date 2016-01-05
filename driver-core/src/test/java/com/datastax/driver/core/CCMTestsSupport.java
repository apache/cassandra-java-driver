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

import com.datastax.driver.core.CCMTestMode.TestMode;
import com.datastax.driver.core.policies.AddressTranslater;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.datastax.driver.core.CCMBridge.IP_PREFIX;
import static com.datastax.driver.core.CCMTestMode.TestMode.PER_CLASS;
import static com.datastax.driver.core.CCMTestMode.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.*;
import static com.datastax.driver.core.Token.M3PToken.FACTORY;

@SuppressWarnings("unused")
public class CCMTestsSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCMTestsSupport.class);

    private static final AtomicInteger CCM_COUNTER = new AtomicInteger(1);

    private static final AtomicInteger KS_COUNTER = new AtomicInteger(1);

    // use ports in the ephemeral range
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(50000);

    private static final List<String> TEST_GROUPS = Lists.newArrayList("isolated", "short", "long", "stress", "duration");

    private static final ReentrantLock PORT_LOCK = new ReentrantLock();

    private static final Function<CCMNode, InetSocketAddress> HOST_ADDRESS = new Function<CCMNode, InetSocketAddress>() {
        @Override
        public InetSocketAddress apply(CCMNode input) {
            return input.hostAddress;
        }
    };

    private static class CCMNode {

        private final int dc;
        private final int n;

        private final long initialToken;
        private final int thriftPort;
        private final int storagePort;
        private final int binaryPort;
        private final int jmxPort;
        private final int remoteDebugPort;

        private final InetSocketAddress hostAddress;

        private CCMNode(int dc, int n, long initialToken, int thriftPort, int storagePort, int binaryPort, int jmxPort, int remoteDebugPort) {
            this.dc = dc;
            this.n = n;
            this.initialToken = initialToken;
            this.thriftPort = thriftPort;
            this.storagePort = storagePort;
            this.binaryPort = binaryPort;
            this.jmxPort = jmxPort;
            this.remoteDebugPort = remoteDebugPort;
            String ip = IP_PREFIX + n;
            InetAddress addr;
            try {
                addr = InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot resolve address: " + ip, e);
            }
            this.hostAddress = new InetSocketAddress(addr, binaryPort);
        }

        @Override
        public String toString() {
            return hostAddress.toString();
        }
    }

    private static class CCMTestConfig {

        private final CCMTestContextKey key;
        private final CCMConfig methodAnnotation;
        private final CCMConfig classAnnotation;
        private final boolean createCluster;
        private final boolean createSession;
        private final boolean createKeyspace;
        private boolean dirtiesContext;

        public CCMTestConfig(
                CCMConfig methodAnnotation,
                CCMConfig classAnnotation,
                CCMBridge.Builder ccmBridgeBuilder,
                int[] numberOfNodes,
                boolean createCluster,
                boolean createSession,
                boolean createKeyspace,
                boolean dirtiesContext) {
            // force no nodes and not started, and disable thrift
            ccmBridgeBuilder
                    .withoutNodes()
                    .notStarted()
                    .withCassandraConfiguration("start_rpc", false);
            boolean multiNode = numberOfNodes.length > 1 || numberOfNodes[0] > 1;
            // for multi nodes, enable PropertyFileSnitch
            if (multiNode) {
                ccmBridgeBuilder.withCassandraConfiguration("endpoint_snitch", "org.apache.cassandra.locator.PropertyFileSnitch");
            }
            key = new CCMTestContextKey(ccmBridgeBuilder, numberOfNodes);
            this.methodAnnotation = methodAnnotation;
            this.classAnnotation = classAnnotation;
            this.createCluster = createCluster;
            this.createSession = createSession;
            this.createKeyspace = createKeyspace;
            this.dirtiesContext = dirtiesContext;
        }

    }

    private static class CCMTestContextKey {

        private final CCMBridge.Builder ccmBridgeBuilder;
        private final int[] numberOfNodes;

        public CCMTestContextKey(CCMBridge.Builder ccmBridgeBuilder, int... numberOfNodes) {
            this.ccmBridgeBuilder = ccmBridgeBuilder;
            this.numberOfNodes = numberOfNodes;
        }

        private CCMTestContext createAndStartTestContext() {
            CCMTestContext ccmTestContext = new CCMTestContext(ccmBridgeBuilder, numberOfNodes);
            ccmTestContext.start();
            return ccmTestContext;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CCMTestContextKey that = (CCMTestContextKey) o;
            return ccmBridgeBuilder.equals(that.ccmBridgeBuilder) && Arrays.equals(numberOfNodes, that.numberOfNodes);

        }

        @Override
        public int hashCode() {
            int result = ccmBridgeBuilder.hashCode();
            result = 31 * result + Arrays.hashCode(numberOfNodes);
            return result;
        }
    }

    private static class CCMTestContext {

        private final int id;

        private final CCMBridge.Builder ccmBridgeBuilder;

        private final int[] numberOfNodes;

        private CCMBridge ccmBridge;

        private List<CCMNode> nodes;

        private boolean keepLogs = false;

        private boolean stopped = false;

        public CCMTestContext(CCMBridge.Builder ccmBridgeBuilder, int... numberOfNodes) {
            this.id = CCM_COUNTER.getAndIncrement();
            this.ccmBridgeBuilder = ccmBridgeBuilder;
            this.numberOfNodes = numberOfNodes;
            this.keepLogs = false;
        }

        private void start() {
            try {
                LOGGER.debug("Starting " + this);
                ccmBridge = ccmBridgeBuilder.build();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        CCMTestContext.this.stop();
                    }
                });
                nodes = createNodes();
                for (CCMNode node : nodes) {
                    ccmBridge.addNode(
                            node.dc,
                            node.n,
                            node.initialToken,
                            node.thriftPort,
                            node.storagePort,
                            node.binaryPort,
                            node.jmxPort,
                            node.remoteDebugPort);
                }
                boolean multiNode = numberOfNodes.length > 1 || numberOfNodes[0] > 1;
                if (multiNode) {
                    Properties topology = createTopology();
                    for (CCMNode node : nodes) {
                        writeTopologyFile(node, topology);
                    }
                }
                ccmBridge.start();
                LOGGER.debug("Started " + this);
            } catch (RuntimeException e) {
                keepLogs = true;
                throw e;
            }
        }

        private synchronized void stop() {
            if (stopped)
                return;
            LOGGER.debug("Stopping " + this);
            if (keepLogs) {
                executeNoFail(new Runnable() {
                    @Override
                    public void run() {
                        ccmBridge.stop();
                    }
                });
                LOGGER.info("Error during tests, kept C* logs in " + ccmBridge.getCcmDir());
            } else {
                executeNoFail(new Runnable() {
                    @Override
                    public void run() {
                        ccmBridge.remove();
                    }
                });
                executeNoFail(new Runnable() {
                    @Override
                    public void run() {
                        org.assertj.core.util.Files.delete(ccmBridge.getCcmDir());
                    }
                });
            }
            stopped = true;
            LOGGER.debug("Stopped " + this);
        }

        private List<CCMNode> createNodes() {
            List<CCMNode> nodes = new ArrayList<CCMNode>();
            PORT_LOCK.lock();
            try {
                // storage port must be the same for all nodes in CCM cluster;
                // find an available port for first ip, and hope it will be available for all ips
                int storagePort = nextPort(1);
                int n = 1;
                for (int dc = 1; dc <= numberOfNodes.length; dc++) {
                    int nodesInDc = numberOfNodes[dc - 1];
                    List<TokenRange> ranges = new TokenRange(FACTORY.minToken(), FACTORY.minToken(), FACTORY).splitEvenly(nodesInDc);
                    for (int i = 0; i < nodesInDc; i++) {
                        // this mimics the computation done by ccm itself,
                        // see https://github.com/pcmanus/ccm/blob/16ecca6d58434ce927af1ff9485c510bd633a140/ccmlib/cluster.py#L188-L210
                        long initialToken = ((Long) ranges.get(i).getStart().getValue()) + (dc - 1) * 100;
                        CCMNode node = new CCMNode(dc, n, initialToken, nextPort(n), storagePort, nextPort(n), nextPort(n), nextPort(n));
                        nodes.add(node);
                        n++;
                    }
                }
            } finally {
                PORT_LOCK.unlock();
            }
            return nodes;
        }

        private int nextPort(int n) {
            int port = TestUtils.findAvailablePort(IP_PREFIX + n, NEXT_PORT.get());
            NEXT_PORT.set(port + 1);
            return port;
        }

        private Properties createTopology() {
            Properties topology = new Properties();
            topology.put("default", "dc1:r1");
            for (CCMNode node : nodes) {
                String ip = node.hostAddress.getAddress().getHostAddress();
                String placement = String.format("dc%s:r1", node.dc);
                topology.setProperty(ip, placement);
            }
            return topology;
        }

        private void writeTopologyFile(CCMNode node, Properties props) {
            Closer closer = Closer.create();
            try {
                File confDir = ccmBridge.getNodeConfDir(node.n);
                File topologyFile = new File(confDir, "cassandra-topology.properties");
                Files.createParentDirs(topologyFile);
                FileWriter writer = new FileWriter(topologyFile);
                closer.register(writer);
                props.store(writer, "Topology for " + ccmBridge);
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

        @Override
        public String toString() {
            return String.format("CCMTestContext %s: %s", id, nodes);
        }
    }

    private static class CCMTestContextLoader extends CacheLoader<CCMTestContextKey, CCMTestContext> {

        @Override
        public CCMTestContext load(CCMTestContextKey key) {
            return key.createAndStartTestContext();
        }

    }

    private static class CCMTestContextRemovalListener implements RemovalListener<CCMTestContextKey, CCMTestContext> {

        @Override
        public void onRemoval(RemovalNotification<CCMTestContextKey, CCMTestContext> notification) {
            if (notification.getValue() != null) {
                notification.getValue().stop();
            }
        }

    }

    /**
     * A LoadingCache that stores running CCM clusters.
     */
    private static final LoadingCache<CCMTestContextKey, CCMTestContext> CACHE = CacheBuilder.newBuilder()
            .initialCapacity(5)
            .maximumSize(10)
            .removalListener(new CCMTestContextRemovalListener())
            .build(new CCMTestContextLoader());

    private TestMode testMode;

    private CCMTestConfig ccmTestConfig;

    private CCMTestContext ccmTestContext;

    protected Cluster cluster;

    protected Session session;

    protected String keyspace;

    private boolean erroredOut = false;

    /**
     * Hook invoked at the beginning of a test class to initialize CCM test context.
     *
     * @throws Exception
     */
    @BeforeClass(groups = {"isolated", "short", "long", "stress", "duration"})
    public void beforeTestClass() throws Exception {
        beforeTestClass(this);
    }

    /**
     * Hook invoked at the beginning of a test class to initialize CCM test context.
     * <p/>
     * Useful when this class is not a superclass of the test being run.
     *
     * @throws Exception
     */
    public void beforeTestClass(Object testInstance) throws Exception {
        this.testMode = determineTestMode(testInstance.getClass());
        if (testMode == PER_CLASS) {
            initTestContext(testInstance, null);
            initTestCluster(testInstance);
            initTestSession();
            initTestKeyspace();
            populateTestKeyspace(testInstance);
        }
    }

    /**
     * Hook executed before each test method.
     *
     * @throws Exception
     */
    @BeforeMethod(groups = {"isolated", "short", "long", "stress", "duration"})
    public void beforeTestMethod(Method testMethod) throws Exception {
        beforeTestMethod(this, testMethod);
    }

    /**
     * Hook executed before each test method.
     * <p/>
     * Useful when this class is not a superclass of the test being run.
     *
     * @throws Exception
     */
    public void beforeTestMethod(Object testInstance, Method testMethod) throws Exception {
        if (testMode == PER_METHOD || erroredOut) {
            initTestContext(testInstance, testMethod);
            initTestCluster(testInstance);
            initTestSession();
            initTestKeyspace();
            populateTestKeyspace(testInstance);
        }
        assert ccmTestConfig != null;
        assert ccmTestContext != null;
        LOGGER.debug("Using " + ccmTestContext);
    }

    /**
     * Hook executed after each test method.
     */
    @AfterMethod(groups = {"isolated", "short", "long", "stress", "duration"}, alwaysRun = true)
    public void afterTestMethod(ITestResult tr) {
        // there seems to be a confusion about the meaning of alwaysRun = true:
        // it makes this method be called on test failures (which is what we want) AND for any test group,
        // even those not listed in the groups attribute above, hence the extra check below
        if (!Collections.disjoint(Arrays.asList(tr.getMethod().getGroups()), TEST_GROUPS)) {
            if (!tr.isSuccess()) {
                errorOut();
            }
            if (testMode == PER_METHOD || erroredOut || ccmTestConfig.dirtiesContext) {
                closeTestCluster();
                cleanUpAfterTest();
            }
        }
    }

    /**
     * Hook executed after each test class.
     */
    @AfterClass(groups = {"isolated", "short", "long", "stress", "duration"}, alwaysRun = true)
    public void afterTestClass() {
        if (testMode == PER_CLASS) {
            closeTestCluster();
            cleanUpAfterTest();
        }
    }

    /**
     * Returns the CCMBridge builder to use for this test.
     * <p/>
     * The default implementation returns a standard CCM cluster configuration;
     * user-defined functions are enabled for protocol versions >= 4.
     * <p/>
     * Note that builders should not add nodes at this stage
     * (i.e. do not call {@link CCMBridge.Builder#withNodes(int...)}),
     * and should not make the resulting CCMBridge auto-startable
     * (i.e., call {@link CCMBridge.Builder#notStarted()}.
     *
     * @return The CCM config to use for the tests.
     */
    public CCMBridge.Builder createCCMBridgeBuilder() {
        CCMBridge.Builder builder = CCMBridge.builder().withoutNodes().notStarted();
        ProtocolVersion protocolVersion = TestUtils.getDesiredProtocolVersion();
        if (protocolVersion.toInt() >= 4) {
            builder = builder.withCassandraConfiguration("enable_user_defined_functions", "true");
        }
        return builder;
    }

    /**
     * Returns the cluster builder to use for this test.
     * <p/>
     * The default implementation returns a vanilla builder.
     * <p/>
     * It's not required to call {@link com.datastax.driver.core.Cluster.Builder#addContactPointsWithPorts},
     * it will be done automatically.
     *
     * @return The cluster builder to use for the tests.
     */
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder();
    }

    /**
     * Returns an alternate cluster builder to use for this test,
     * with no event debouncing.
     * <p/>
     * It's not required to call {@link com.datastax.driver.core.Cluster.Builder#addContactPointsWithPorts},
     * it will be done automatically.
     *
     * @return The cluster builder to use for the tests.
     */
    public Cluster.Builder createClusterBuilderNoDebouncing() {
        return Cluster.builder().withQueryOptions(TestUtils.nonDebouncingQueryOptions());
    }

    /**
     * The CQL statements to execute before the tests in this class.
     * Useful to create tables or other objects inside the test keyspace,
     * or to insert test data.
     * <p/>
     * Statements do not need to be qualified with keyspace name.
     * <p/>
     * The default implementation returns an empty list (no fixtures required).
     *
     * @return The DDL statements to execute before the tests.
     */
    public Collection<String> createTestFixtures() {
        return Collections.emptyList();
    }

    /**
     * Returns the {@link CCMBridge} instance being used for the current test.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @return the {@link CCMBridge} instance being used for the current test.
     */
    public CCMBridge ccmBridge() {
        assert ccmTestContext != null;
        return ccmTestContext.ccmBridge;
    }

    /**
     * Signals that the test has encountered an unexpected error.
     * <p/>
     * This method is automatically called when a test finishes with an unexpected exception
     * being thrown, but it is also possible to manually invoke it.
     * <p/>
     * Calling this method will close the current cluster and session.
     * The CCM data directory will be kept after the test session is finished,
     * for debugging purposes
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     */
    public void errorOut() {
        assert ccmTestContext != null;
        erroredOut = true;
        ccmTestContext.keepLogs = true;
    }

    /**
     * Returns the contact points to use to contact the CCM cluster.
     * This method returns as many contact points as the number of nodes in the CCM cluster.
     * On a multi-DC setup, this will include nodes in all data centers.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @return the contact points to use to contact the CCM cluster.
     */
    public List<InetSocketAddress> getContactPoints() {
        assert ccmTestContext != null;
        return Lists.transform(ccmTestContext.nodes, HOST_ADDRESS);
    }

    /**
     * Returns the address of the {@code nth} host in the CCM cluster (counting from 1, i.e.,
     * {@code getHostAddress(1)} returns the address of the first node.
     * <p/>
     * In multi-DC setups, nodes are numbered in ascending order of their datacenter number.
     * E.g. with 2 DCs and 3 nodes in each DC, the first node in DC 2 is number 4.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @return the address of the {@code nth} host in the cluster.
     */
    public InetSocketAddress getHostAddress(int n) {
        return getContactPoints().get(n - 1);
    }

    /**
     * @return The cassandra version used by CCM clusters during this test session.
     */
    public VersionNumber getCassandraVersion() {
        return VersionNumber.parse(CCMBridge.getCassandraVersion());
    }

    private void initTestContext(Object testInstance, Method testMethod) throws Exception {
        erroredOut = false;
        ccmTestConfig = createCCMTestConfig(testInstance, testMethod);
        assert ccmTestConfig != null;
        try {
            ccmTestContext = CACHE.get(ccmTestConfig.key);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
        }
        assert ccmTestContext != null;
    }

    private void initTestCluster(Object testInstance) throws Exception {
        if (ccmTestConfig.createCluster) {
            Cluster.Builder builder = createClusterBuilder(ccmTestConfig.methodAnnotation, ccmTestConfig.classAnnotation, testInstance);
            cluster = builder
                    .addContactPointsWithPorts(getContactPoints())
                    // we need an address translator because
                    // the native ports used are not the standard ones
                    .withAddressTranslater(new AddressTranslater() {
                        @Override
                        public InetSocketAddress translate(InetSocketAddress address) {
                            for (CCMNode node : ccmTestContext.nodes) {
                                if (node.hostAddress.getAddress().equals(address.getAddress()))
                                    return node.hostAddress;
                            }
                            throw new IllegalArgumentException("Unknown host: " + address);
                        }
                    })
                    .build();
        }
    }

    private void initTestSession() throws Exception {
        if (ccmTestConfig.createCluster && ccmTestConfig.createSession)
            session = cluster.connect();
    }

    private void initTestKeyspace() {
        if (ccmTestConfig.createCluster && ccmTestConfig.createSession && ccmTestConfig.createKeyspace) {
            try {
                keyspace = SIMPLE_KEYSPACE + "_" + KS_COUNTER.getAndIncrement();
                LOGGER.debug("Using keyspace " + keyspace);
                session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
                session.execute("USE " + keyspace);
            } catch (Exception e) {
                errorOut();
                LOGGER.error("Could not create test keyspace", e);
                Throwables.propagate(e);
            }
        }
    }

    private void populateTestKeyspace(Object testInstance) throws Exception {
        if (ccmTestConfig.createCluster && ccmTestConfig.createSession && ccmTestConfig.createKeyspace) {
            for (String stmt : createFixtures(ccmTestConfig.methodAnnotation, ccmTestConfig.classAnnotation, testInstance)) {
                try {
                    session.execute(stmt);
                } catch (Exception e) {
                    errorOut();
                    LOGGER.error("Could not execute statement: " + stmt, e);
                    Throwables.propagate(e);
                }
            }
        }
    }

    private void cleanUpAfterTest() {
        if (ccmTestConfig != null && ccmTestConfig.dirtiesContext) {
            CACHE.invalidate(ccmTestConfig.key);
            assert ccmTestContext.stopped;
        }
        ccmTestConfig = null;
        ccmTestContext = null;
    }

    private void closeTestCluster() {
        if (cluster != null)
            executeNoFail(new Runnable() {
                @Override
                public void run() {
                    cluster.close();
                }
            });
        cluster = null;
        session = null;
        keyspace = null;
    }

    private static CCMTestConfig createCCMTestConfig(Object testInstance, Method testMethod) throws Exception {
        CCMConfig methodAnnotation = locateAnnotation(testMethod, CCMConfig.class);
        CCMConfig classAnnotation = locateAnnotation(testInstance.getClass(), CCMConfig.class);
        return new CCMTestConfig(methodAnnotation, classAnnotation,
                createCCMBridgeBuilder(methodAnnotation, classAnnotation, testInstance),
                determineNumberOfNodes(methodAnnotation, classAnnotation),
                determineCreateCluster(methodAnnotation, classAnnotation),
                determineCreateSession(methodAnnotation, classAnnotation),
                determineCreateKeyspace(methodAnnotation, classAnnotation),
                determineDirtiesContext(methodAnnotation, classAnnotation)
        );
    }

    private static TestMode determineTestMode(Class<?> testClass) {
        CCMTestMode ann = locateAnnotation(testClass, CCMTestMode.class);
        if (ann != null)
            return ann.value();
        return PER_CLASS;
    }

    private static int[] determineNumberOfNodes(CCMConfig methodAnnotation, CCMConfig classAnnotation) {
        if (methodAnnotation != null && methodAnnotation.numberOfNodes().length > 0)
            return methodAnnotation.numberOfNodes();
        if (classAnnotation != null && classAnnotation.numberOfNodes().length > 0)
            return classAnnotation.numberOfNodes();
        return new int[]{1};
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean determineCreateCluster(CCMConfig methodAnnotation, CCMConfig classAnnotation) {
        if (methodAnnotation != null && methodAnnotation.createCluster().length == 1)
            return methodAnnotation.createCluster()[0];
        if (classAnnotation != null && classAnnotation.createCluster().length == 1)
            return classAnnotation.createCluster()[0];
        return true;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean determineCreateSession(CCMConfig methodAnnotation, CCMConfig classAnnotation) {
        if (methodAnnotation != null && methodAnnotation.createSession().length == 1)
            return methodAnnotation.createSession()[0];
        if (classAnnotation != null && classAnnotation.createSession().length == 1)
            return classAnnotation.createSession()[0];
        return true;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean determineCreateKeyspace(CCMConfig methodAnnotation, CCMConfig classAnnotation) {
        if (methodAnnotation != null && methodAnnotation.createKeyspace().length == 1)
            return methodAnnotation.createKeyspace()[0];
        if (classAnnotation != null && classAnnotation.createKeyspace().length == 1)
            return classAnnotation.createKeyspace()[0];
        return true;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean determineDirtiesContext(CCMConfig methodAnnotation, CCMConfig classAnnotation) {
        if (methodAnnotation != null && methodAnnotation.dirtiesContext().length == 1)
            return methodAnnotation.dirtiesContext()[0];
        if (classAnnotation != null && classAnnotation.dirtiesContext().length == 1)
            return classAnnotation.dirtiesContext()[0];
        return false;
    }

    private static CCMBridge.Builder createCCMBridgeBuilder(CCMConfig methodAnnotation, CCMConfig classAnnotation, Object testInstance) throws Exception {
        String methodName = null;
        Class<?> clazz = null;
        if (methodAnnotation != null) {
            if (!methodAnnotation.ccmProvider().isEmpty()) {
                methodName = methodAnnotation.ccmProvider();
            }
            if (!methodAnnotation.ccmProviderClass().equals(CCMConfig.Undefined.class)) {
                clazz = methodAnnotation.ccmProviderClass();
            }
        }
        if (classAnnotation != null) {
            if (methodName == null && !classAnnotation.ccmProvider().isEmpty()) {
                methodName = classAnnotation.ccmProvider();
            }
            if (clazz == null && !classAnnotation.ccmProviderClass().equals(CCMConfig.Undefined.class)) {
                clazz = classAnnotation.ccmProviderClass();
            }
        }
        if (methodName == null)
            methodName = "createCCMBridgeBuilder";
        if (clazz == null)
            clazz = testInstance.getClass();
        Method method = locateMethod(methodName, clazz);
        assert CCMBridge.Builder.class.isAssignableFrom(method.getReturnType());
        if (Modifier.isStatic(method.getModifiers())) {
            return (CCMBridge.Builder) method.invoke(null);
        } else {
            Object receiver = testInstance.getClass().equals(clazz) ? testInstance : instantiate(clazz);
            return (CCMBridge.Builder) method.invoke(receiver);
        }
    }

    private static Cluster.Builder createClusterBuilder(CCMConfig methodAnnotation, CCMConfig classAnnotation, Object testInstance) throws Exception {
        String methodName = null;
        Class<?> clazz = null;
        if (methodAnnotation != null) {
            if (!methodAnnotation.clusterProvider().isEmpty()) {
                methodName = methodAnnotation.clusterProvider();
            }
            if (!methodAnnotation.clusterProviderClass().equals(CCMConfig.Undefined.class)) {
                clazz = methodAnnotation.clusterProviderClass();
            }
        }
        if (classAnnotation != null) {
            if (methodName == null && !classAnnotation.clusterProvider().isEmpty()) {
                methodName = classAnnotation.clusterProvider();
            }
            if (clazz == null && !classAnnotation.clusterProviderClass().equals(CCMConfig.Undefined.class)) {
                clazz = classAnnotation.clusterProviderClass();
            }
        }
        if (methodName == null)
            methodName = "createClusterBuilder";
        if (clazz == null)
            clazz = testInstance.getClass();
        Method method = locateMethod(methodName, clazz);
        assert Cluster.Builder.class.isAssignableFrom(method.getReturnType());
        if (Modifier.isStatic(method.getModifiers())) {
            return (Cluster.Builder) method.invoke(null);
        } else {
            Object receiver = testInstance.getClass().equals(clazz) ? testInstance : instantiate(clazz);
            return (Cluster.Builder) method.invoke(receiver);
        }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> createFixtures(CCMConfig methodAnnotation, CCMConfig classAnnotation, Object testInstance) throws Exception {
        String methodName = null;
        Class<?> clazz = null;
        if (methodAnnotation != null) {
            if (!methodAnnotation.fixturesProvider().isEmpty()) {
                methodName = methodAnnotation.fixturesProvider();
            }
            if (!methodAnnotation.fixturesProviderClass().equals(CCMConfig.Undefined.class)) {
                clazz = methodAnnotation.fixturesProviderClass();
            }
        }
        if (classAnnotation != null) {
            if (methodName == null && !classAnnotation.fixturesProvider().isEmpty()) {
                methodName = classAnnotation.fixturesProvider();
            }
            if (clazz == null && !classAnnotation.fixturesProviderClass().equals(CCMConfig.Undefined.class)) {
                clazz = classAnnotation.fixturesProviderClass();
            }
        }
        if (methodName == null)
            methodName = "createTestFixtures";
        if (clazz == null)
            clazz = testInstance.getClass();
        Method method = locateMethod(methodName, clazz);
        assert Collection.class.isAssignableFrom(method.getReturnType());
        assert method.getGenericReturnType() instanceof ParameterizedType;
        assert String.class.equals(((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0]);
        if (Modifier.isStatic(method.getModifiers())) {
            return (Collection<String>) method.invoke(null);
        } else {
            Object receiver = testInstance.getClass().equals(clazz) ? testInstance : instantiate(clazz);
            return (Collection<String>) method.invoke(receiver);
        }
    }

    private static <A extends Annotation> A locateAnnotation(Method testMethod, Class<? extends A> clazz) {
        if (testMethod == null)
            return null;
        testMethod.setAccessible(true);
        return testMethod.getAnnotation(clazz);
    }

    private static <A extends Annotation> A locateAnnotation(Class<?> clazz, Class<? extends A> annotationClass) {
        A ann = clazz.getAnnotation(annotationClass);
        if (ann != null)
            return ann;
        clazz = clazz.getSuperclass();
        if (clazz == null)
            return null;
        return locateAnnotation(clazz, annotationClass);
    }

    private static Method locateMethod(String methodName, Class<?> clazz) throws NoSuchMethodException {
        try {
            Method method = clazz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            clazz = clazz.getSuperclass();
            if (clazz == null)
                throw e;
            return locateMethod(methodName, clazz);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T instantiate(Class<? extends T> clazz) throws NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        if (clazz.getEnclosingClass() == null || Modifier.isStatic(clazz.getModifiers())) {
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (T) constructor.newInstance();
        } else {
            Class<?> enclosingClass = clazz.getEnclosingClass();
            Object enclosingInstance = enclosingClass.newInstance();
            Constructor<?> constructor = clazz.getDeclaredConstructor(enclosingClass);
            constructor.setAccessible(true);
            return (T) constructor.newInstance(enclosingInstance);
        }
    }

}
