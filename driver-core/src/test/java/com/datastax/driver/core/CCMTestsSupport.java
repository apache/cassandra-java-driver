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

import com.datastax.driver.core.CCMAccess.Workload;
import com.datastax.driver.core.CreateCCM.TestMode;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestResult;
import org.testng.annotations.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_CLASS;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.*;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("unused")
public class CCMTestsSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCMTestsSupport.class);

    private static final AtomicInteger CCM_COUNTER = new AtomicInteger(1);

    private static final List<String> TEST_GROUPS = Lists.newArrayList("isolated", "short", "long", "stress", "duration");

    // A mapping of cassandra.yaml config options to their version requirements.
    // If a config is passed containing one of these options and the version requirement cannot be met
    // the option is simply filtered.
    private static final Map<String, VersionNumber> configVersionRequirements = ImmutableMap.<String, VersionNumber>builder()
            .put("enable_user_defined_functions", VersionNumber.parse("2.2.0"))
            .build();

    private static class ReadOnlyCCMAccess implements CCMAccess {

        private final CCMAccess delegate;

        private ReadOnlyCCMAccess(CCMAccess delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getClusterName() {
            return delegate.getClusterName();
        }

        @Override
        public VersionNumber getCassandraVersion() {
            return delegate.getCassandraVersion();
        }

        @Override
        public VersionNumber getDSEVersion() {
            return delegate.getDSEVersion();
        }

        @Override
        public InetSocketAddress addressOfNode(int n) {
            return delegate.addressOfNode(n);
        }

        @Override
        public File getCcmDir() {
            return delegate.getCcmDir();
        }

        @Override
        public File getClusterDir() {
            return delegate.getClusterDir();
        }

        @Override
        public File getNodeDir(int n) {
            return delegate.getNodeDir(n);
        }

        @Override
        public File getNodeConfDir(int n) {
            return delegate.getNodeConfDir(n);
        }

        @Override
        public int getStoragePort() {
            return delegate.getStoragePort();
        }

        @Override
        public int getThriftPort() {
            return delegate.getThriftPort();
        }

        @Override
        public int getBinaryPort() {
            return delegate.getBinaryPort();
        }

        @Override
        public void setKeepLogs(boolean keepLogs) {
            delegate.setKeepLogs(keepLogs);
        }

        @Override
        public String checkForErrors() {
            return delegate.checkForErrors();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void stop() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void forceStop() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void start(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void stop(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void forceStop(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void pause(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void resume(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void remove(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void add(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void add(int dc, int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void decommission(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateConfig(Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateDSEConfig(Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateNodeConfig(int n, String key, Object value) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateNodeConfig(int n, Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateDSENodeConfig(int n, String key, Object value) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateDSENodeConfig(int n, Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void setWorkload(int node, Workload... workload) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void waitForUp(int node) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void waitForDown(int node) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            return delegate.getProtocolVersion();
        }

        @Override
        public ProtocolVersion getProtocolVersion(ProtocolVersion maximumAllowed) {
            return delegate.getProtocolVersion(maximumAllowed);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    private static class CCMTestConfig {

        private final List<CCMConfig> annotations;

        private CCMBridge.Builder ccmBuilder;

        public CCMTestConfig(List<CCMConfig> annotations) {
            this.annotations = annotations;
        }

        private int[] numberOfNodes() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.numberOfNodes().length > 0)
                    return ann.numberOfNodes();
            }
            return new int[]{1};
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private String version() {
            for (CCMConfig ann : annotations) {
                if (ann != null && !ann.version().isEmpty())
                    return ann.version();
            }
            return null;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Boolean dse() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.dse().length > 0)
                    return ann.dse()[0];
            }
            return null;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean ssl() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.ssl().length > 0)
                    return ann.ssl()[0];
            }
            return false;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean auth() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.auth().length > 0)
                    return ann.auth()[0];
            }
            return false;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Map<String, Object> config() {
            Map<String, Object> config = new HashMap<String, Object>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                addConfigOptions(ann.config(), config);
            }
            return config;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Map<String, Object> dseConfig() {
            Map<String, Object> config = new HashMap<String, Object>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                addConfigOptions(ann.dseConfig(), config);
            }
            return config;
        }

        private void addConfigOptions(String[] conf, Map<String, Object> config) {
            VersionNumber version = VersionNumber.parse(version());
            if (version == null) {
                version = CCMBridge.getGlobalCassandraVersion();
            } else {
                Boolean dse = dse();
                if (dse != null && dse) {
                    version = CCMBridge.getCassandraVersion(version);
                }
            }
            for (String aConf : conf) {
                String[] tokens = aConf.split(":");
                if (tokens.length != 2)
                    fail("Wrong configuration option: " + aConf);
                String key = tokens[0];
                String value = tokens[1];
                // If we've detected a property with a version requirement, skip it if the version requirement
                // cannot be met.
                if (configVersionRequirements.containsKey(key)) {
                    VersionNumber requirement = configVersionRequirements.get(key);
                    if (version != null && version.compareTo(requirement) < 0) {
                        LOGGER.debug("Skipping inclusion of '{}' in cassandra.yaml since it requires >= C* {} and {} " +
                                "was detected.", aConf, requirement, version);
                        continue;
                    }
                }
                config.put(key, value);
            }
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Set<String> jvmArgs() {
            Set<String> args = new LinkedHashSet<String>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                Collections.addAll(args, ann.jvmArgs());
            }
            return args;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Set<String> startOptions() {
            Set<String> args = new LinkedHashSet<String>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                Collections.addAll(args, ann.options());
            }
            return args;
        }

        private List<Workload[]> workloads() {
            int total = 0;
            for (int perDc : numberOfNodes())
                total += perDc;
            List<Workload[]> workloads = new ArrayList<Workload[]>(Collections.<Workload[]>nCopies(total, null));
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                CCMWorkload[] annWorkloads = ann.workloads();
                for (int j = 0; j < annWorkloads.length; j++) {
                    CCMWorkload nodeWorkloads = annWorkloads[j];
                    workloads.set(j, nodeWorkloads.value());
                }
            }
            return workloads;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createCcm() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createCcm().length > 0)
                    return ann.createCcm()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createCluster() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createCluster().length > 0)
                    return ann.createCluster()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createSession() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createSession().length > 0)
                    return ann.createSession()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createKeyspace() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createKeyspace().length > 0)
                    return ann.createKeyspace()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean dirtiesContext() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.dirtiesContext().length > 0)
                    return ann.dirtiesContext()[0];
            }
            return false;
        }

        private CCMBridge.Builder ccmBuilder(Object testInstance) throws Exception {
            if (ccmBuilder == null) {
                ccmBuilder = ccmProvider(testInstance);
                if (ccmBuilder == null) {
                    ccmBuilder = CCMBridge.builder().withNodes(numberOfNodes()).notStarted();
                }

                String versionStr = version();
                if (versionStr != null) {
                    VersionNumber version = VersionNumber.parse(versionStr);
                    ccmBuilder.withVersion(version);
                }

                Boolean dse = dse();
                if (dse != null)
                    ccmBuilder.withDSE(dse);
                if (ssl())
                    ccmBuilder.withSSL();
                if (auth())
                    ccmBuilder.withAuth();
                for (Map.Entry<String, Object> entry : config().entrySet()) {
                    ccmBuilder.withCassandraConfiguration(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : dseConfig().entrySet()) {
                    ccmBuilder.withDSEConfiguration(entry.getKey(), entry.getValue());
                }
                for (String option : startOptions()) {
                    ccmBuilder.withCreateOptions(option);
                }
                for (String arg : jvmArgs()) {
                    ccmBuilder.withJvmArgs(arg);
                }
                List<Workload[]> workloads = workloads();
                for (int i = 0; i < workloads.size(); i++) {
                    Workload[] workload = workloads.get(i);
                    if (workload != null)
                        ccmBuilder.withWorkload(i + 1, workload);
                }
            }
            return ccmBuilder;
        }

        private CCMBridge.Builder ccmProvider(Object testInstance) throws Exception {
            String methodName = null;
            Class<?> clazz = null;
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                if (!ann.ccmProvider().isEmpty()) {
                    methodName = ann.ccmProvider();
                }
                if (!ann.ccmProviderClass().equals(CCMConfig.Undefined.class)) {
                    clazz = ann.ccmProviderClass();
                }
            }
            if (methodName == null)
                return null;
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

        private Cluster.Builder clusterProvider(Object testInstance) throws Exception {
            String methodName = null;
            Class<?> clazz = null;
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                if (!ann.clusterProvider().isEmpty()) {
                    methodName = ann.clusterProvider();
                }
                if (!ann.clusterProviderClass().equals(CCMConfig.Undefined.class)) {
                    clazz = ann.clusterProviderClass();
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
        private void invokeInitTest(Object testInstance) throws Exception {
            String methodName = null;
            Class<?> clazz = null;
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                if (!ann.testInitializer().isEmpty()) {
                    methodName = ann.testInitializer();
                }
                if (!ann.testInitializerClass().equals(CCMConfig.Undefined.class)) {
                    clazz = ann.testInitializerClass();
                }
            }
            if (methodName == null)
                methodName = "onTestContextInitialized";
            if (clazz == null)
                clazz = testInstance.getClass();
            Method method = locateMethod(methodName, clazz);
            if (Modifier.isStatic(method.getModifiers())) {
                method.invoke(null);
            } else {
                Object receiver = testInstance.getClass().equals(clazz) ? testInstance : instantiate(clazz);
                method.invoke(receiver);
            }
        }

    }

    private TestMode testMode;

    protected CCMTestConfig ccmTestConfig;

    private CCMAccess ccm;

    private CCMBridge.Builder ccmBuilder;

    private Cluster cluster;

    private Session session;

    protected String keyspace;

    private boolean erroredOut = false;

    private Closer closer;

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
        testMode = determineTestMode(testInstance.getClass());
        if (testMode == PER_CLASS) {
            closer = Closer.create();
            try {
                initTestContext(testInstance, null);
                initTestCluster(testInstance);
                initTestSession();
                initTestKeyspace();
                initTest(testInstance);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                errorOut();
                fail(e.getMessage());

            }
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
        if (isCcmEnabled(testMethod)) {
            if (closer == null)
                closer = Closer.create();
            if (testMode == PER_METHOD || erroredOut) {
                try {
                    initTestContext(testInstance, testMethod);
                    initTestCluster(testInstance);
                    initTestSession();
                    initTestKeyspace();
                    initTest(testInstance);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    errorOut();
                    fail(e.getMessage());
                }
            }
            assert ccmTestConfig != null;
            assert !ccmTestConfig.createCcm() || ccm != null;
        }
    }

    /**
     * Hook executed after each test method.
     */
    @AfterMethod(groups = {"isolated", "short", "long", "stress", "duration"}, alwaysRun = true)
    public void afterTestMethod(ITestResult tr) throws Exception {
        if (isCcmEnabled(tr.getMethod().getConstructorOrMethod().getMethod())) {
            if (tr.getStatus() == ITestResult.FAILURE) {
                errorOut();
            }
            if (erroredOut || testMode == PER_METHOD) {
                closeCloseables();
                closeTestCluster();
            }
            if (testMode == PER_METHOD)
                closeTestContext();
        }
    }

    /**
     * Hook executed after each test class.
     */
    @AfterClass(groups = {"isolated", "short", "long", "stress", "duration"}, alwaysRun = true)
    public void afterTestClass() throws Exception {
        if (testMode == PER_CLASS) {
            closeCloseables();
            closeTestCluster();
            closeTestContext();
        }
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
        return Cluster.builder()
                // use a different codec registry for each cluster instance
                .withCodecRegistry(new CodecRegistry());
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
     * Hook invoked when the test context is ready, but before the test itself.
     * Useful to create fixtures or to insert test data.
     * <p/>
     * This method is invoked once per class is the {@link TestMode} is {@link TestMode#PER_CLASS}
     * or before each test method, if the  {@link TestMode} is {@link TestMode#PER_METHOD}.
     * <p/>
     * When this method is called, the cluster and the session are ready
     * to be used (unless the configuration specifies that such objects
     * should not be created).
     * <p/>
     * Statements executed inside this method do not need to be qualified with a keyspace name,
     * in which case they are executed with the default keyspace for the test
     * (unless the configuration specifies that no keyspace should be creaed for the test).
     * <p/>
     * The default implementation does nothing (no fixtures required).
     */
    public void onTestContextInitialized() {
        // nothing to do by default
    }

    /**
     * @return The {@link CCMAccess} instance to use with this test.
     */
    public CCMAccess ccm() {
        return ccm;
    }

    /**
     * @return The {@link Cluster} instance to use with this test.
     * Can be null if CCM configuration specifies {@code createCluster = false}.
     */
    public Cluster cluster() {
        return cluster;
    }

    /**
     * @return The {@link Session} instance to use with this test.
     * Can be null if CCM configuration specifies {@code createSession = false}.
     */
    public Session session() {
        return session;
    }

    /**
     * Executes the given statements with the test's session object.
     * <p/>
     * Useful to create test fixtures and/or load data before tests.
     * <p/>
     * This method should not be called if a session object hasn't been created
     * (if CCM configuration specifies {@code createSession = false})
     *
     * @param statements The statements to execute.
     */
    public void execute(String... statements) {
        execute(Arrays.asList(statements));
    }

    /**
     * Executes the given statements with the test's session object.
     * <p/>
     * Useful to create test fixtures and/or load data before tests.
     * <p/>
     * This method should not be called if a session object hasn't been created
     * (if CCM configuration specifies {@code createSession = false})
     *
     * @param statements The statements to execute.
     */
    public void execute(Collection<String> statements) {
        assert session != null;
        for (String stmt : statements) {
            try {
                session.execute(stmt);
            } catch (Exception e) {
                errorOut();
                LOGGER.error("Could not execute statement: " + stmt, e);
                Throwables.propagate(e);
            }
        }
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
        erroredOut = true;
        if (ccm != null) {
            ccm.setKeepLogs(true);
        }
    }

    /**
     * Returns the contact points to use to contact the CCM cluster.
     * <p/>
     * This method returns as many contact points as the number of nodes initially present in the CCM cluster,
     * according to {@link CCMConfig} annotations.
     * <p/>
     * On a multi-DC setup, this will include nodes in all data centers.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @return the contact points to use to contact the CCM cluster.
     */
    public List<InetAddress> getContactPoints() {
        assert ccmTestConfig != null;
        List<InetAddress> contactPoints = new ArrayList<InetAddress>();
        int n = 1;
        int[] numberOfNodes = ccmTestConfig.numberOfNodes();
        for (int dc = 1; dc <= numberOfNodes.length; dc++) {
            int nodesInDc = numberOfNodes[dc - 1];
            for (int i = 0; i < nodesInDc; i++) {
                try {
                    contactPoints.add(InetAddress.getByName(ipOfNode(n)));
                } catch (UnknownHostException e) {
                    Throwables.propagate(e);
                }
                n++;
            }
        }
        return contactPoints;
    }

    /**
     * Returns the contact points to use to contact the CCM cluster.
     * <p/>
     * This method returns as many contact points as the number of nodes initially present in the CCM cluster,
     * according to {@link CCMConfig} annotations.
     * <p/>
     * On a multi-DC setup, this will include nodes in all data centers.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @return the contact points to use to contact the CCM cluster.
     */
    public List<InetSocketAddress> getContactPointsWithPorts() {
        assert ccmTestConfig != null;
        List<InetSocketAddress> contactPoints = new ArrayList<InetSocketAddress>();
        int n = 1;
        int[] numberOfNodes = ccmTestConfig.numberOfNodes();
        for (int dc = 1; dc <= numberOfNodes.length; dc++) {
            int nodesInDc = numberOfNodes[dc - 1];
            for (int i = 0; i < nodesInDc; i++) {
                contactPoints.add(new InetSocketAddress(ipOfNode(n), ccm.getBinaryPort()));
                n++;
            }
        }
        return contactPoints;
    }

    /**
     * Registers the given {@link Closeable} to be closed at the end of the current test method.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @param closeable The closeable to close
     * @return The closeable to close
     */
    public <T extends Closeable> T register(T closeable) {
        closer.register(closeable);
        return closeable;
    }

    /**
     * Tests fail randomly with InvalidQueryException: Keyspace 'xxx' does not exist;
     * this method tries at most 3 times to issue a successful USE statement.
     *
     * @param ks The keyspace to USE
     */
    public void useKeyspace(String ks) {
        useKeyspace(session(), ks);
    }

    /**
     * Tests fail randomly with InvalidQueryException: Keyspace 'xxx' does not exist;
     * this method tries at most 3 times to issue a successful USE statement.
     *
     * @param session The session to use
     * @param ks      The keyspace to USE
     */
    public void useKeyspace(Session session, String ks) {
        final int maxTries = 3;
        for (int i = 1; i <= maxTries; i++) {
            try {
                session.execute("USE " + ks);
            } catch (InvalidQueryException e) {
                if (i == maxTries)
                    throw e;
                LOGGER.error("Could not USE keyspace, retrying");
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES);
            }
        }
    }

    protected void initTestContext(Object testInstance, Method testMethod) throws Exception {
        erroredOut = false;
        ccmTestConfig = createCCMTestConfig(testInstance, testMethod);
        assert ccmTestConfig != null;
        if (ccmTestConfig.createCcm()) {
            ccmBuilder = ccmTestConfig.ccmBuilder(testInstance);
            CCMAccess ccm = CCMCache.get(ccmBuilder);
            assert ccm != null;
            if (ccmTestConfig.dirtiesContext()) {
                this.ccm = ccm;
            } else {
                this.ccm = new ReadOnlyCCMAccess(ccm);
            }
            try {
                ccm.start();
            } catch (CCMException e) {
                errorOut();
                fail(e.getMessage());
            }
            LOGGER.debug("Using {}", ccm);
        }
    }

    protected void initTestCluster(Object testInstance) throws Exception {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster()) {
            Cluster.Builder builder = ccmTestConfig.clusterProvider(testInstance);
            // add contact points only if the provided builder didn't do so
            if (builder.getContactPoints().isEmpty())
                builder.addContactPoints(getContactPoints());
            builder.withPort(ccm.getBinaryPort());
            cluster = register(builder.build());
            cluster.init();
        }
    }

    protected void initTestSession() throws Exception {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster() && ccmTestConfig.createSession())
            session = register(cluster.connect());
    }

    protected void initTestKeyspace() {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster() && ccmTestConfig.createSession() && ccmTestConfig.createKeyspace()) {
            try {
                keyspace = TestUtils.generateIdentifier("ks_");
                LOGGER.debug("Using keyspace " + keyspace);
                session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
                useKeyspace(keyspace);
            } catch (Exception e) {
                errorOut();
                LOGGER.error("Could not create test keyspace", e);
                Throwables.propagate(e);
            }
        }
    }

    protected void initTest(Object testInstance) throws Exception {
        ccmTestConfig.invokeInitTest(testInstance);
    }

    protected void closeTestContext() throws Exception {
        if (ccmTestConfig != null && ccmBuilder != null && ccm != null) {
            if (ccmTestConfig.dirtiesContext()) {
                CCMCache.remove(ccmBuilder);
                ccm.close();
            } else {
                ((ReadOnlyCCMAccess) ccm).delegate.close();
            }
        }
        ccmTestConfig = null;
        ccmBuilder = null;
        ccm = null;
    }

    protected void closeTestCluster() {
        if (cluster != null && !cluster.isClosed())
            executeNoFail(new Runnable() {
                @Override
                public void run() {
                    cluster.close();
                }
            }, false);
        cluster = null;
        session = null;
        keyspace = null;
    }

    protected void closeCloseables() {
        if (closer != null)
            executeNoFail(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    closer.close();
                    return null;
                }
            }, false);
    }

    private static boolean isCcmEnabled(Method testMethod) {
        Test ann = locateAnnotation(testMethod, Test.class);
        return !Collections.disjoint(Arrays.asList(ann.groups()), TEST_GROUPS);
    }

    private static CCMTestConfig createCCMTestConfig(Object testInstance, Method testMethod) throws Exception {
        ArrayList<CCMConfig> annotations = new ArrayList<CCMConfig>();
        CCMConfig ann = locateAnnotation(testMethod, CCMConfig.class);
        if (ann != null)
            annotations.add(ann);
        locateClassAnnotations(testInstance.getClass(), CCMConfig.class, annotations);
        return new CCMTestConfig(annotations);
    }

    private static TestMode determineTestMode(Class<?> testClass) {
        List<CreateCCM> annotations = locateClassAnnotations(testClass, CreateCCM.class, new ArrayList<CreateCCM>());
        if (!annotations.isEmpty())
            return annotations.get(0).value();
        return PER_CLASS;
    }

    private static <A extends Annotation> A locateAnnotation(Method testMethod, Class<? extends A> clazz) {
        if (testMethod == null)
            return null;
        testMethod.setAccessible(true);
        return testMethod.getAnnotation(clazz);
    }

    private static <A extends Annotation> List<A> locateClassAnnotations(Class<?> clazz, Class<? extends A> annotationClass, List<A> annotations) {
        A ann = clazz.getAnnotation(annotationClass);
        if (ann != null)
            annotations.add(ann);
        clazz = clazz.getSuperclass();
        if (clazz == null)
            return annotations;
        return locateClassAnnotations(clazz, annotationClass, annotations);
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
