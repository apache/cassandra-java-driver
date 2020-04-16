/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.osgi.support;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
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
                "com.datastax.oss",
                "java-driver-shaded-guava",
                getVersionFromDepsTxt("com.datastax.oss:java-driver-shaded-guava")),
            mavenBundle(
                "io.dropwizard.metrics",
                "metrics-core",
                getVersionFromSystemProperty("metrics.version")),
            mavenBundle("org.slf4j", "slf4j-api", getVersionFromSystemProperty("slf4j.version")),
            mavenBundle(
                "org.hdrhistogram",
                "HdrHistogram",
                getVersionFromSystemProperty("hdrhistogram.version")),
            mavenBundle("com.typesafe", "config", getVersionFromSystemProperty("config.version")),
            mavenBundle(
                "com.datastax.oss",
                "native-protocol",
                getVersionFromDepsTxt("com.datastax.oss:native-protocol")),
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
            + getVersionFromSystemProperty("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverCoreShadedBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../core-shaded/target/java-driver-core-shaded-"
            + getVersionFromSystemProperty("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverQueryBuilderBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../query-builder/target/java-driver-query-builder-"
            + getVersionFromSystemProperty("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverTestInfraBundle() {
    return bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../test-infra/target/java-driver-test-infra-"
            + getVersionFromSystemProperty("project.version")
            + ".jar");
  }

  public static CompositeOption testBundles() {
    return () ->
        options(
            driverTestInfraBundle(),
            simulacronBundles(),
            awaitilityBundles(),
            nettyBundles(), // required by the test infra bundle, even for the shaded jar
            jacksonBundles(), // required by the Simulacron bundle, even for the shaded jar
            mavenBundle(
                "org.apache.commons",
                "commons-exec",
                getVersionFromSystemProperty("commons-exec.version")),
            mavenBundle(
                "org.assertj", "assertj-core", getVersionFromSystemProperty("assertj.version")),
            junitBundles());
  }

  public static CompositeOption nettyBundles() {
    String nettyVersion = getVersionFromSystemProperty("netty.version");
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
    String logbackVersion = getVersionFromSystemProperty("logback.version");
    return () ->
        options(
            mavenBundle("ch.qos.logback", "logback-classic", logbackVersion),
            mavenBundle("ch.qos.logback", "logback-core", logbackVersion));
  }

  public static CompositeOption jacksonBundles() {
    String jacksonVersion = getVersionFromSystemProperty("jackson.version");
    String jacksonDatabindVersion = getVersionFromSystemProperty("jackson-databind.version");
    return () ->
        options(
            mavenBundle("com.fasterxml.jackson.core", "jackson-databind", jacksonDatabindVersion),
            mavenBundle("com.fasterxml.jackson.core", "jackson-core", jacksonVersion),
            mavenBundle("com.fasterxml.jackson.core", "jackson-annotations", jacksonVersion));
  }

  public static CompositeOption simulacronBundles() {
    String simulacronVersion = getVersionFromSystemProperty("simulacron.version");
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

  public static CompositeOption awaitilityBundles() {
    String awaitilityVersion = getVersionFromSystemProperty("awaitility.version");
    return () ->
        options(
            mavenBundle("org.awaitility", "awaitility", awaitilityVersion),
            mavenBundle("org.hamcrest", "hamcrest", "2.1"));
  }

  public static MavenArtifactProvisionOption lz4Bundle() {
    return mavenBundle("org.lz4", "lz4-java", getVersionFromSystemProperty("lz4.version"));
  }

  public static MavenArtifactProvisionOption snappyBundle() {
    return mavenBundle(
        "org.xerial.snappy", "snappy-java", getVersionFromSystemProperty("snappy.version"));
  }

  public static CompositeOption tinkerpopBundles() {
    String mavenVersion = getVersionFromSystemProperty("tinkerpop.version");
    String osgiVersion = toOsgiTinkerpopVersion(mavenVersion);
    return () ->
        options(
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "gremlin-core", mavenVersion))
                .exports(
                    // avoid exporting 'org.apache.tinkerpop.gremlin.*' as other Tinkerpop jars have
                    // this root package as well
                    "org.apache.tinkerpop.gremlin.jsr223.*",
                    "org.apache.tinkerpop.gremlin.process.*",
                    "org.apache.tinkerpop.gremlin.structure.*",
                    "org.apache.tinkerpop.gremlin.util.*")
                .bundleVersion(osgiVersion)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-core")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "gremlin-driver", mavenVersion))
                .exports("org.apache.tinkerpop.gremlin.driver.*")
                .bundleVersion(osgiVersion)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-driver")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "tinkergraph-gremlin", mavenVersion))
                .exports("org.apache.tinkerpop.gremlin.tinkergraph.*")
                .bundleVersion(osgiVersion)
                .bundleSymbolicName("org.apache.tinkerpop.tinkergraph-gremlin")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    mavenBundle("org.apache.tinkerpop", "gremlin-shaded", mavenVersion))
                .exports("org.apache.tinkerpop.shaded.*")
                .bundleVersion(osgiVersion)
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
                        "com.esri.geometry",
                        "esri-geometry-api",
                        getVersionFromSystemProperty("esri.version")))
                .exports("com.esri.core.geometry.*")
                .imports("org.json", "org.codehaus.jackson")
                .bundleVersion(getVersionFromSystemProperty("esri.version"))
                .bundleSymbolicName("com.esri.core.geometry")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            mavenBundle("org.json", "json", getVersionFromSystemProperty("json.version")),
            mavenBundle(
                "org.codehaus.jackson",
                "jackson-core-asl",
                getVersionFromSystemProperty("legacy-jackson.version")));
  }

  public static CompositeOption reactiveBundles() {
    return () ->
        options(
            mavenBundle(
                "org.reactivestreams",
                "reactive-streams",
                getVersionFromSystemProperty("reactive-streams.version")),
            mavenBundle(
                "io.reactivex.rxjava2", "rxjava", getVersionFromSystemProperty("rxjava.version")));
  }

  private static String getVersionFromSystemProperty(String propertyName) {
    String value = System.getProperty(propertyName);
    if (value == null) {
      throw new IllegalArgumentException(propertyName + " system property is not set");
    }
    return value;
  }

  /**
   * Some versions are not available as system properties because they are hardcoded in the BOM.
   *
   * <p>Rely on the deps.txt file instead.
   */
  private static String getVersionFromDepsTxt(String searchString) {
    for (String dependency : DepsTxtLoader.lines) {
      if (dependency.contains(searchString)) {
        List<String> components = Splitter.on(':').splitToList(dependency);
        return components.get(components.size() - 2);
      }
    }
    throw new IllegalStateException("Couldn't find version for " + searchString);
  }

  private static class DepsTxtLoader {

    private static List<String> lines;

    static {
      String path =
          PathUtils.getBaseDir()
              + "/../core/target/classes/com/datastax/dse/driver/internal/deps.txt";
      CharSource charSource = Files.asCharSource(new File(path), Charsets.UTF_8);

      try {
        lines = charSource.readLines();
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Couldn't load deps.txt for driver core, "
                + "make sure you run `mvn generate-resources` before running this test",
            e);
      }
    }
  }

  private static String toOsgiTinkerpopVersion(String inVersion) {

    Version inVersionObj = Version.parse(inVersion);
    return String.join(
        ".",
        Integer.toString(inVersionObj.getMajor()),
        Integer.toString(inVersionObj.getMinor()),
        Integer.toString(inVersionObj.getPatch()));
  }
}
