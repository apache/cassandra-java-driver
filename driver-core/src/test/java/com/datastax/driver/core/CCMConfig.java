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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The configuration to use when running tests with {@link CCMTestsSupport}.
 * <p/>
 * This annotation can be used at the class level, or at the method level if you annotate the class with
 * {@code @CreateCCM(PER_METHOD)}.
 */
@Retention(RUNTIME)
@Target({TYPE, METHOD})
public @interface CCMConfig {

    final class Undefined {
    }

    /**
     * The number of nodes to create, per data center.
     * If not set, this defaults to {@code {1}}, i.e., one data center with one node.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return The number of nodes to create, per data center.
     */
    int[] numberOfNodes() default {};

    /**
     * The C* or DSE version to use; defaults to the version defined by
     * the System property {@code cassandra.version}.
     * <p/>
     * Note that setting this attribute completely
     * overrides the System properties {@code cassandra.version}
     * and {@code cassandra.directory}.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return The C* or DSE version to use
     * @see CCMBridge#getCassandraVersion()
     */
    String version() default "";

    /**
     * Whether to launch a DSE instance rather than an OSS C*.
     * <p/>
     * Note that setting this attribute completely
     * overrides the System property {@code dse}.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return {@code true} to launch a DSE instance, {@code false} to launch an OSS C* instance (default).
     */
    boolean[] dse() default {};

    /**
     * Configuration items to add to cassandra.yaml configuration file.
     * Each configuration item must be in the form {@code key:value}.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return Configuration items to add to cassandra.yaml configuration file.
     */
    String[] config() default {};

    /**
     * Configuration items to add to dse.yaml configuration file.
     * Each configuration item must be in the form {@code key:value}.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return Configuration items to add to dse.yaml configuration file.
     */
    String[] dseConfig() default {};

    /**
     * JVM args to use when starting hosts.
     * System properties should be provided one by one, in the form
     * {@code -Dname=value}.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return JVM args to use when starting hosts.
     */
    String[] jvmArgs() default {};

    /**
     * Free-form options that will be added at the end of the {@code ccm create} command.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return Free-form options that will be added at the end of the {@code ccm create} command.
     */
    String[] options() default {};

    /**
     * Whether to use SSL encryption.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return {@code true} to use encryption, {@code false} to use unencrypted communication (default).
     */
    boolean[] ssl() default {};

    /**
     * Whether to use authentication. Implies the use of SSL encryption.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return {@code true} to use authentication, {@code false} to use unauthenticated communication (default).
     */
    boolean[] auth() default {};

    /**
     * The workloads to assign to each node. If this attribute is defined,
     * the number of its elements must be lesser than or equal to the number of nodes in the cluster.
     * <p/>
     * This attribute is ignored if {@link #ccmProvider()} is defined.
     *
     * @return The workloads to assign to each node.
     */
    CCMWorkload[] workloads() default {};

    /**
     * Returns {@code true} if a {@link CCMBridge} instance should be automatically created, {@code false} otherwise.
     * <p/>
     * If {@code true}, a CCM cluster builder will be created or retrieved from the cache.
     * If {@code false}, no CCM cluster will be activated for this test; this can be useful
     * when some tests in a test class require CCM, while others don't but are in one of the allowed
     * CCM test groups ("short" for example).
     * <p/>
     * The cluster will be created once for the whole class if CCM test mode is {@link CreateCCM.TestMode#PER_CLASS},
     * or once per test method, if the CCM test mode is {@link CreateCCM.TestMode#PER_METHOD},
     *
     * @return {@code true} if a {@link Cluster} instance should be automatically created, {@code false} otherwise.
     */
    boolean[] createCcm() default {};

    /**
     * Returns {@code true} if a {@link Cluster} instance should be automatically created, {@code false} otherwise.
     * <p/>
     * If {@code true}, a cluster builder will be obtained by invoking the method
     * specified by {@link #clusterProvider()} and {@link #clusterProviderClass()}.
     * <p/>
     * The CCM cluster will be created once for the whole class if CCM test mode is {@link CreateCCM.TestMode#PER_CLASS},
     * or once per test method, if the CCM test mode is {@link CreateCCM.TestMode#PER_METHOD},
     *
     * @return {@code true} if a {@link Cluster} instance should be automatically created, {@code false} otherwise.
     */
    boolean[] createCluster() default {};

    /**
     * Returns {@code true} if a {@link Session} instance should be automatically created, {@code false} otherwise.
     * <p/>
     * If {@code true}, a cluster builder will be obtained by invoking the method
     * specified by {@link #clusterProvider()} and {@link #clusterProviderClass()},
     * and then the session will be created through {@link Cluster#connect()}.
     * <p/>
     * The session will be created once for the whole class if CCM test mode is {@link CreateCCM.TestMode#PER_CLASS},
     * or once per test method, if the CCM test mode is {@link CreateCCM.TestMode#PER_METHOD},
     *
     * @return {@code true} if a {@link Session} instance should be automatically created, {@code false} otherwise.
     */
    boolean[] createSession() default {};

    /**
     * Returns {@code true} if a test keyspace should be automatically created, {@code false} otherwise.
     * <p/>
     * If {@code true}, a cluster builder will be obtained by invoking the method
     * specified by {@link #clusterProvider()} and {@link #clusterProviderClass()},
     * then a session will be created through {@link Cluster#connect()},
     * and this session will be used to create the test keyspace.
     * <p/>
     * The keyspace will be created once for the whole class if CCM test mode is {@link CreateCCM.TestMode#PER_CLASS},
     * or once per test method, if the CCM test mode is {@link CreateCCM.TestMode#PER_METHOD}.
     * <p/>
     * The test keyspace will be automatically populated upon creation with fixtures provided by
     * {@link #testInitializer()} and {@link #testInitializerClass()}.
     *
     * @return {@code true} if a test keyspace should be automatically created, {@code false} otherwise.
     */
    boolean[] createKeyspace() default {};

    /**
     * Returns {@code true} if the test class or the test method alters the CCM cluster,
     * e.g. by adding or removing nodes,
     * in which case, it should not be reused after the test is finished.
     * <p/>
     * If {@code true}, CCM cluster will be destroyed after the test method if test mode is
     * {@link CreateCCM.TestMode#PER_METHOD}, or after the test class, if test mode
     * is {@link CreateCCM.TestMode#PER_CLASS}.
     *
     * @return {@code true} if the test class or the test method alters the CCM cluster used,
     * {@code false} otherwise.
     */
    boolean[] dirtiesContext() default {};

    /**
     * Returns the name of the method that should be invoked to obtain
     * a {@link com.datastax.driver.core.CCMBridge.Builder} instance.
     * <p/>
     * This method should be declared in {@link #ccmProviderClass()},
     * or if that attribute is not set,
     * it will be looked up on the test class itself.
     * <p/>
     * The method should not have parameters. It can be static or not,
     * and have any visibility.
     * <p/>
     * By default, a {@link com.datastax.driver.core.CCMBridge.Builder} instance
     * is obtained by parsing other attributes of this annotation.
     *
     * @return The name of the method that should be invoked to obtain a
     * {@link com.datastax.driver.core.CCMBridge.Builder} instance.
     */
    String ccmProvider() default "";

    /**
     * Returns the name of the class that should be invoked to obtain
     * a {@link com.datastax.driver.core.CCMBridge.Builder} instance.
     * <p/>
     * This class should contain a method named after {@link #ccmProvider()};
     * if this attribute is not set,
     * it will default to the test class itself.
     *
     * @return The name of the class that should be invoked to obtain a
     * {@link com.datastax.driver.core.CCMBridge.Builder} instance.
     */
    Class<?> ccmProviderClass() default Undefined.class;

    /**
     * Returns the name of the method that should be invoked to obtain
     * a {@link com.datastax.driver.core.Cluster.Builder} instance.
     * <p/>
     * This method should be declared in {@link #clusterProviderClass()},
     * or if that attribute is not set,
     * it will be looked up on the test class itself.
     * <p/>
     * The method should not have parameters. It can be static or not,
     * and have any visibility.
     * <p/>
     * By default, the test will look for a method named after {@code createClusterBuilder}.
     *
     * @return The name of the method that should be invoked to obtain a
     * {@link com.datastax.driver.core.Cluster.Builder} instance.
     */
    String clusterProvider() default "";

    /**
     * Returns the name of the class that should be invoked to obtain
     * a {@link com.datastax.driver.core.Cluster.Builder} instance.
     * <p/>
     * This class should contain a method named after {@link #clusterProvider()};
     * if this attribute is not set,
     * it will default to the test class itself.
     *
     * @return The name of the class that should be invoked to obtain a
     * {@link com.datastax.driver.core.Cluster.Builder} instance.
     */
    Class<?> clusterProviderClass() default Undefined.class;

    /**
     * Returns the name of the method that should be invoked once the test context
     * is initialized.
     * <p/>
     * This method should be declared in {@link #testInitializerClass()},
     * or if that attribute is not set,
     * it will be looked up on the test class itself.
     * <p/>
     * The method should not have parameters. It can be static or not,
     * and have any visibility.
     * <p/>
     * By default, the framework will look for a method named
     * {@code onTestContextInitialized}.
     *
     * @return The name of the method that should be invoked once the test context
     * is initialized.
     */
    String testInitializer() default "";

    /**
     * Returns the name of the class that should be invoked once the test context
     * is initialized.
     * <p/>
     * This class should contain a method named after {@link #testInitializer()};
     * if this attribute is not set,
     * it will default to the test class itself.
     *
     * @return The name of the class that should be invoked once the test context
     * is initialized.
     */
    Class<?> testInitializerClass() default Undefined.class;

}
