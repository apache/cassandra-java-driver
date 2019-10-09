/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.osgi.support;

import static com.datastax.oss.driver.osgi.BundleOptions.getVersion;

import com.datastax.oss.driver.osgi.BundleOptions;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption.OverwriteMode;
import org.ops4j.pax.exam.util.PathUtils;

public class DseBundleOptions {

  public static UrlProvisionOption driverDseBundle() {
    return CoreOptions.bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../core/target/dse-java-driver-core-"
            + getVersion("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverDseShadedBundle() {
    return CoreOptions.bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../core-shaded/target/dse-java-driver-core-shaded-"
            + getVersion("project.version")
            + ".jar");
  }

  public static UrlProvisionOption driverDseQueryBuilderBundle() {
    return CoreOptions.bundle(
        "reference:file:"
            + PathUtils.getBaseDir()
            + "/../query-builder/target/dse-java-driver-query-builder-"
            + getVersion("project.version")
            + ".jar");
  }

  public static MavenArtifactProvisionOption driverCoreBundle() {
    return CoreOptions.mavenBundle(
        "com.datastax.oss", "java-driver-core", getVersion("oss-driver.version"));
  }

  public static MavenArtifactProvisionOption driverCoreShadedBundle() {
    return CoreOptions.mavenBundle(
        "com.datastax.oss", "java-driver-core-shaded", getVersion("oss-driver.version"));
  }

  public static MavenArtifactProvisionOption driverQueryBuilderBundle() {
    return CoreOptions.mavenBundle(
        "com.datastax.oss", "java-driver-query-builder", getVersion("oss-driver.version"));
  }

  private static MavenArtifactProvisionOption driverTestInfraBundle() {
    return CoreOptions.mavenBundle(
        "com.datastax.oss", "java-driver-test-infra", getVersion("oss-driver.version"));
  }

  public static CompositeOption baseOptions() {
    // In theory, the options declared here should only include bundles that must be present
    // in order for both the non-shaded and shaded driver versions to work properly.
    // Bundles that should be present only for the non-shaded driver version should be declared
    // elsewhere (e.g. ESRI, legacy "Codehaus" Jackson). Also, bundles that have optional resolution
    // should be declared elsewhere (e.g. Tinkerpop, Reactive Streams).
    // However we have two exceptions: Netty and modern "FasterXML" Jackson; both are shaded, but
    // need to be present in all cases because the test bundles requires their presence (see
    // #testBundles method).
    return () ->
        CoreOptions.options(
            CoreOptions.mavenBundle(
                "com.datastax.oss", "java-driver-shaded-guava", getVersion("guava.version")),
            CoreOptions.mavenBundle(
                "io.dropwizard.metrics", "metrics-core", getVersion("metrics.version")),
            CoreOptions.mavenBundle("org.slf4j", "slf4j-api", getVersion("slf4j.version")),
            CoreOptions.mavenBundle(
                "org.hdrhistogram", "HdrHistogram", getVersion("hdrhistogram.version")),
            CoreOptions.mavenBundle("com.typesafe", "config", getVersion("config.version")),
            CoreOptions.mavenBundle(
                "com.datastax.oss", "native-protocol", getVersion("native-protocol.version")),
            CoreOptions.mavenBundle(
                "com.datastax.dse",
                "dse-native-protocol",
                getVersion("dse-native-protocol.version")),
            BundleOptions.logbackBundles(),
            CoreOptions.systemProperty("logback.configurationFile")
                .value("file:" + PathUtils.getBaseDir() + "/src/test/resources/logback-test.xml"),
            testBundles());
  }

  public static CompositeOption tinkerpopBundles() {
    String version = System.getProperty("tinkerpop.version");
    return () ->
        CoreOptions.options(
            CoreOptions.wrappedBundle(
                    CoreOptions.mavenBundle("org.apache.tinkerpop", "gremlin-core", version))
                .exports(
                    // avoid exporting 'org.apache.tinkerpop.gremlin.*' as other Tinkerpop jars have
                    // this root package as well
                    "org.apache.tinkerpop.gremlin.jsr223.*",
                    "org.apache.tinkerpop.gremlin.process.*",
                    "org.apache.tinkerpop.gremlin.structure.*",
                    "org.apache.tinkerpop.gremlin.util.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-core")
                .overwriteManifest(OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    CoreOptions.mavenBundle("org.apache.tinkerpop", "gremlin-driver", version))
                .exports("org.apache.tinkerpop.gremlin.driver.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-driver")
                .overwriteManifest(OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    CoreOptions.mavenBundle("org.apache.tinkerpop", "tinkergraph-gremlin", version))
                .exports("org.apache.tinkerpop.gremlin.tinkergraph.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.tinkergraph-gremlin")
                .overwriteManifest(OverwriteMode.FULL),
            CoreOptions.wrappedBundle(
                    CoreOptions.mavenBundle("org.apache.tinkerpop", "gremlin-shaded", version))
                .exports("org.apache.tinkerpop.shaded.*")
                .bundleVersion(version)
                .bundleSymbolicName("org.apache.tinkerpop.gremlin-shaded")
                .overwriteManifest(OverwriteMode.FULL),
            // Note: the versions below are hard-coded because they shouldn't change very often,
            // but if the tests fail because of them, we should consider parameterizing them
            CoreOptions.mavenBundle("commons-configuration", "commons-configuration", "1.10"),
            CoreOptions.mavenBundle("commons-collections", "commons-collections", "3.2.2"),
            CoreOptions.mavenBundle("org.apache.commons", "commons-lang3", "3.8.1"),
            CoreOptions.mavenBundle("commons-lang", "commons-lang", "2.6"),
            CoreOptions.wrappedBundle(
                    CoreOptions.mavenBundle("org.javatuples", "javatuples", "1.2"))
                .exports("org.javatuples.*")
                .bundleVersion("1.2")
                .bundleSymbolicName("org.javatuples")
                .overwriteManifest(OverwriteMode.FULL));
  }

  private static CompositeOption testBundles() {
    return () ->
        CoreOptions.options(
            driverTestInfraBundle(),
            BundleOptions.simulacronBundles(),
            BundleOptions
                .nettyBundles(), // required by the test infra bundle, even for the shaded jar
            BundleOptions
                .jacksonBundles(), // required by the Simulacron bundle, even for the shaded jar
            CoreOptions.mavenBundle(
                "org.apache.commons", "commons-exec", System.getProperty("commons-exec.version")),
            CoreOptions.mavenBundle(
                "org.assertj", "assertj-core", System.getProperty("assertj.version")),
            CoreOptions.junitBundles());
  }

  public static CompositeOption esriBundles() {
    return () ->
        CoreOptions.options(
            CoreOptions.wrappedBundle(
                    CoreOptions.mavenBundle(
                        "com.esri.geometry", "esri-geometry-api", getVersion("esri.version")))
                .exports("com.esri.core.geometry.*")
                .imports("org.json", "org.codehaus.jackson")
                .bundleVersion(getVersion("esri.version"))
                .bundleSymbolicName("com.esri.core.geometry")
                .overwriteManifest(OverwriteMode.FULL),
            CoreOptions.mavenBundle("org.json", "json", getVersion("json.version")),
            CoreOptions.mavenBundle(
                "org.codehaus.jackson", "jackson-core-asl", getVersion("legacy-jackson.version")));
  }

  public static CompositeOption reactiveBundles() {
    return () ->
        CoreOptions.options(
            CoreOptions.mavenBundle(
                "org.reactivestreams", "reactive-streams", getVersion("reactive-streams.version")),
            CoreOptions.mavenBundle(
                "io.reactivex.rxjava2", "rxjava", getVersion("rxjava.version")));
  }
}
