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
package com.datastax.oss.driver.osgi.support;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;

import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;
import org.ops4j.pax.exam.util.PathUtils;

public class BundleOptions {

  public static CompositeOption baseOptions() {
    // In theory, the options declared here should only include bundles that must be present
    // in order for both the non-shaded and shaded driver versions to work properly.
    // Bundles that should be present only for the non-shaded driver version should be declared
    // elsewhere.
    // However we have two exceptions: Netty and FasterXML Jackson; both need to be present in all
    // cases because the test bundles requires their presence (see #testBundles method).
    return () ->
        options(
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
            nettyBundles(), // required by the test infra bundle, even for the shaded jar
            jacksonBundles(), // required by the Simulacron bundle, even for the shaded jar
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
    String jacksonDatabindVersion = getVersion("jackson-databind.version");
    return () ->
        options(
            mavenBundle("com.fasterxml.jackson.core", "jackson-databind", jacksonDatabindVersion),
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

  public static CompositeOption tinkerpopBundles() {
    String version = System.getProperty("tinkerpop.version");
    return () ->
        options(
            CoreOptions.wrappedBundle(mavenBundle("org.apache.tinkerpop", "gremlin-core", version))
                .exports(
                    // avoid exporting 'org.apache.tinkerpop.gremlin.*' as other Tinkerpop jars have
                    // this root package as well
                    "org.apache.tinkerpop.gremlin.jsr223.*",
                    "org.apache.tinkerpop.gremlin.process.*",
                    "org.apache.tinkerpop.gremlin.structure.*",
                    "org.apache.tinkerpop.gremlin.util.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-core")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "gremlin-driver", version))
                .exports("org.apache.tinkerpop.gremlin.driver.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-driver")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "tinkergraph-gremlin", version))
                .exports("org.apache.tinkerpop.gremlin.tinkergraph.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.tinkergraph-gremlin")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "gremlin-shaded", version))
                .exports("org.apache.tinkerpop.shaded.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-shaded")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            // Note: the versions below are hard-coded because they shouldn't change very often,
            // but if the tests fail because of them, we should consider parameterizing them
            mavenBundle("commons-configuration", "commons-configuration", "1.10"),
            mavenBundle("commons-collections", "commons-collections", "3.2.2"),
            mavenBundle("org.apache.commons", "commons-lang3", "3.8.1"),
            mavenBundle("commons-lang", "commons-lang", "2.6"),
            CoreOptions.wrappedBundle(mavenBundle("org.javatuples", "javatuples", "1.2"))
                .exports("org.javatuples.*")
                .bundleVersion("1.2")
                .bundleSymbolicName("org.javatuples")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL));
  }

  public static CompositeOption esriBundles() {
    return () ->
        options(
            CoreOptions.wrappedBundle(
                    mavenBundle(
                        "com.esri.geometry", "esri-geometry-api", getVersion("esri.version")))
                .exports("com.esri.core.geometry.*")
                .imports("org.json", "org.codehaus.jackson")
                .bundleVersion(getVersion("esri.version"))
                .bundleSymbolicName("com.esri.core.geometry")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            mavenBundle("org.json", "json", getVersion("json.version")),
            mavenBundle(
                "org.codehaus.jackson", "jackson-core-asl", getVersion("legacy-jackson.version")));
  }

  public static CompositeOption reactiveBundles() {
    return () ->
        options(
            mavenBundle(
                "org.reactivestreams", "reactive-streams", getVersion("reactive-streams.version")),
            mavenBundle("io.reactivex.rxjava2", "rxjava", getVersion("rxjava.version")));
  }
}
