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
package com.datastax.driver.osgi;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.TestUtils;
import com.google.common.collect.Lists;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.util.PathUtils;

import java.util.List;

import static org.ops4j.pax.exam.CoreOptions.*;

public class BundleOptions {

    public static UrlProvisionOption driverBundle() {
        return driverBundle(false);
    }

    public static UrlProvisionOption driverBundle(boolean useShaded) {
        String classifier = useShaded ? "-shaded" : "";
        return bundle("reference:file:" + PathUtils.getBaseDir() + "/../../driver-core/target/cassandra-driver-core-" + Cluster.getDriverVersion() + classifier + ".jar");
    }

    public static UrlProvisionOption mappingBundle() {
        return bundle("reference:file:" + PathUtils.getBaseDir() + "/../../driver-mapping/target/cassandra-driver-mapping-" + Cluster.getDriverVersion() + ".jar");
    }

    public static UrlProvisionOption extrasBundle() {
        return bundle("reference:file:" + PathUtils.getBaseDir() + "/../../driver-extras/target/cassandra-driver-extras-" + Cluster.getDriverVersion() + ".jar");
    }

    public static MavenArtifactProvisionOption guavaBundle() {
        return mavenBundle("com.google.guava", "guava", getVersion("guava.version"));
    }

    public static CompositeOption lz4Bundle() {
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                return options(
                        systemProperty("cassandra.compression").value(ProtocolOptions.Compression.LZ4.name()),
                        mavenBundle("net.jpountz.lz4", "lz4", getVersion("lz4.version"))
                );
            }
        };
    }

    public static CompositeOption snappyBundle() {
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                return options(
                        systemProperty("cassandra.compression").value(ProtocolOptions.Compression.SNAPPY.name()),
                        mavenBundle("org.xerial.snappy", "snappy-java", getVersion("snappy.version"))
                );
            }
        };
    }

    public static CompositeOption hdrHistogramBundle() {
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                return options(
                        systemProperty("cassandra.usePercentileSpeculativeExecutionPolicy").value("true"),
                        mavenBundle("org.hdrhistogram", "HdrHistogram", getVersion("hdr.version"))
                );
            }
        };
    }

    public static CompositeOption nettyBundles() {
        final String nettyVersion = getVersion("netty.version");
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                return options(
                        mavenBundle("io.netty", "netty-buffer", nettyVersion),
                        mavenBundle("io.netty", "netty-codec", nettyVersion),
                        mavenBundle("io.netty", "netty-common", nettyVersion),
                        mavenBundle("io.netty", "netty-handler", nettyVersion),
                        mavenBundle("io.netty", "netty-transport", nettyVersion)
                );
            }
        };
    }

    public static UrlProvisionOption mailboxBundle() {
        return bundle("reference:file:" + PathUtils.getBaseDir() + "/target/classes");
    }

    public static CompositeOption defaultOptions() {
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                List<Option> options = Lists.newArrayList(
                        // Delegate javax.security.cert to the parent classloader.  javax.security.cert.X509Certificate is used in
                        // io.netty.util.internal.EmptyArrays, but not directly by the driver.
                        bootDelegationPackage("javax.security.cert"),
                        systemProperty("cassandra.version").value(CCMBridge.getGlobalCassandraVersion().toString()),
                        systemProperty("cassandra.contactpoints").value(TestUtils.IP_PREFIX + 1),
                        systemProperty("logback.configurationFile").value("file:" + PathUtils.getBaseDir() + "/src/test/resources/logback.xml"),
                        mavenBundle("org.slf4j", "slf4j-api", getVersion("slf4j.version")),
                        mavenBundle("ch.qos.logback", "logback-classic", getVersion("logback.version")),
                        mavenBundle("ch.qos.logback", "logback-core", getVersion("logback.version")),
                        mavenBundle("io.dropwizard.metrics", "metrics-core", getVersion("metrics.version")),
                        mavenBundle("org.testng", "testng", getVersion("testng.version")),
                        systemPackages("org.testng", "org.junit", "org.junit.runner", "org.junit.runner.manipulation",
                                "org.junit.runner.notification", "com.jcabi.manifests")
                );
                if (CCMBridge.isWindows()) {
                    // Workaround for Felix + Windows Server 2012.   Felix does not properly alias 'windowsserver2012'
                    // to 'win32', because of this some native libraries may fail to load.  To work around this, force
                    // the os.name property to win32 if on a windows platform.
                    // See: https://issues.apache.org/jira/browse/FELIX-5184
                    options.add(systemProperty("os.name").value("win32"));
                }

                return options.toArray(new Option[options.size()]);
            }
        };
    }

    private static String getVersion(String propertyName) {
        String value = System.getProperty(propertyName);
        if (value == null) {
            throw new IllegalArgumentException(propertyName + " system property is not set.");
        }
        return value;
    }
}
