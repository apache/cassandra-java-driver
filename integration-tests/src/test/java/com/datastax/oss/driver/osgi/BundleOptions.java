/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.osgi;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;

import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.util.PathUtils;

public class BundleOptions {

  public static CompositeOption baseOptions() {
    // Note: the bundles below include Netty; these bundles are not required by
    // the shaded core driver bundle, but they need to be present in all cases because
    // the test-infra bundle requires the (non-shaded) Netty bundle.
    return () ->
        options(
            nettyBundles(),
            mavenBundle(
                "com.datastax.oss", "java-driver-shaded-guava", getVersion("guava.version")),
            mavenBundle("io.dropwizard.metrics", "metrics-core", getVersion("metrics.version")),
            mavenBundle("org.slf4j", "slf4j-api", getVersion("slf4j.version")),
            mavenBundle("org.hdrhistogram", "HdrHistogram", getVersion("hdrhistogram.version")),
            mavenBundle("com.typesafe", "config", getVersion("config.version")),
            mavenBundle(
                "com.datastax.oss", "native-protocol", getVersion("native-protocol.version")),
            logbackBundles(),
            systemProperty("logback.configurationFile")
                .value("file:" + PathUtils.getBaseDir() + "/src/test/resources/logback-test.xml"),
            testBundles());
  }

  public static UrlProvisionOption driverCoreBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../core/target/java-driver-core-"
            + getVersion("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverCoreShadedBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../core-shaded/target/java-driver-core-shaded-"
            + getVersion("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverQueryBuilderBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../query-builder/target/java-driver-query-builder-"
            + getVersion("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverTestInfraBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../test-infra/target/java-driver-test-infra-"
            + getVersion("project.version")
            + ".jar");
  }

  public static CompositeOption testBundles() {
    return () ->
        options(
            driverTestInfraBundle(),
            simulacronBundles(),
            jacksonBundles(),
            mavenBundle(
                "org.apache.commons", "commons-exec", System.getProperty("commons-exec.version")),
            mavenBundle("org.assertj", "assertj-core", System.getProperty("assertj.version")),
            junitBundles());
  }

  public static CompositeOption nettyBundles() {
    String nettyVersion = getVersion("netty.version");
    return () ->
        options(
            mavenBundle("io.netty", "netty-handler", nettyVersion),
            mavenBundle("io.netty", "netty-buffer", nettyVersion),
            mavenBundle("io.netty", "netty-codec", nettyVersion),
            mavenBundle("io.netty", "netty-common", nettyVersion),
            mavenBundle("io.netty", "netty-transport", nettyVersion),
            mavenBundle("io.netty", "netty-resolver", nettyVersion));
  }

  public static CompositeOption logbackBundles() {
    String logbackVersion = getVersion("logback.version");
    return () ->
        options(
            mavenBundle("ch.qos.logback", "logback-classic", logbackVersion),
            mavenBundle("ch.qos.logback", "logback-core", logbackVersion));
  }

  public static CompositeOption jacksonBundles() {
    String jacksonVersion = getVersion("jackson.version");
    return () ->
        options(
            mavenBundle("com.fasterxml.jackson.core", "jackson-databind", jacksonVersion),
            mavenBundle("com.fasterxml.jackson.core", "jackson-core", jacksonVersion),
            mavenBundle("com.fasterxml.jackson.core", "jackson-annotations", jacksonVersion));
  }

  public static CompositeOption simulacronBundles() {
    String simulacronVersion = getVersion("simulacron.version");
    return () ->
        options(
            mavenBundle(
                "com.datastax.oss.simulacron", "simulacron-native-server", simulacronVersion),
            mavenBundle("com.datastax.oss.simulacron", "simulacron-common", simulacronVersion),
            mavenBundle(
                "com.datastax.oss.simulacron",
                "simulacron-native-protocol-json",
                simulacronVersion));
  }

  public static MavenArtifactProvisionOption lz4Bundle() {
    return mavenBundle("org.lz4", "lz4-java", getVersion("lz4.version"));
  }

  public static MavenArtifactProvisionOption snappyBundle() {
    return mavenBundle("org.xerial.snappy", "snappy-java", getVersion("snappy.version"));
  }

  public static String getVersion(String propertyName) {
    String value = System.getProperty(propertyName);
    if (value == null) {
      throw new IllegalArgumentException(propertyName + " system property is not set");
    }
    return value;
  }
}
