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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.dse.driver.internal.core.insights.PlatformInfoFinder.UNVERIFIED_RUNTIME_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.RuntimeAndCompileTimeVersions;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class PlatformInfoFinderTest {

  private URL nullUrlProvider(PlatformInfoFinder.DependencyFromFile d) {
    return null;
  }

  private URL nettyUrlProvider(PlatformInfoFinder.DependencyFromFile d) {
    return this.getClass().getResource("/insights/pom.properties");
  }

  private URL malformedUrlProvider(PlatformInfoFinder.DependencyFromFile d) {
    return this.getClass().getResource("/insights/malformed-pom.properties");
  }

  private URL nonExistingUrlProvider(PlatformInfoFinder.DependencyFromFile d) {
    return this.getClass().getResource("/insights/non-existing.pom");
  }

  @Test
  public void should_find_dependencies_from_file() {
    // given
    InputStream inputStream =
        this.getClass().getResourceAsStream("/insights/test-dependencies.txt");
    Map<String, RuntimeAndCompileTimeVersions> expected = new HashMap<>();
    expected.put(
        "io.netty:netty-transport-native-epoll",
        withUnverifiedRuntimeVersionOptional("4.0.56.Final"));
    expected.put("org.slf4j:slf4j-api", withUnverifiedRuntimeVersion("1.7.25"));
    expected.put("org.ow2.asm:asm", withUnverifiedRuntimeVersion("5.0.3"));
    expected.put("com.esri.geometry:esri-geometry-api", withUnverifiedRuntimeVersion("1.2.1"));
    expected.put("io.netty:netty-transport", withUnverifiedRuntimeVersion("4.0.56.Final"));
    expected.put("com.github.jnr:jnr-x86asm", withUnverifiedRuntimeVersion("1.0.2"));
    expected.put("org.ow2.asm:asm-analysis", withUnverifiedRuntimeVersion("5.0.3"));
    expected.put("com.github.jnr:jnr-constants", withUnverifiedRuntimeVersion("0.9.9"));
    expected.put("io.netty:netty-common", withUnverifiedRuntimeVersion("4.0.56.Final"));
    expected.put("com.google.guava:guava", withUnverifiedRuntimeVersion("19.0"));
    expected.put("org.xerial.snappy:snappy-java", withUnverifiedRuntimeVersionOptional("1.1.2.6"));
    expected.put("io.dropwizard.metrics:metrics-core", withUnverifiedRuntimeVersion("3.2.2"));
    expected.put("org.ow2.asm:asm-tree", withUnverifiedRuntimeVersion("5.0.3"));
    expected.put("com.github.jnr:jnr-posix", withUnverifiedRuntimeVersion("3.0.44"));
    expected.put("org.codehaus.jackson:jackson-core-asl", withUnverifiedRuntimeVersion("1.9.12"));
    expected.put(
        "com.fasterxml.jackson.core:jackson-databind", withUnverifiedRuntimeVersion("2.7.9.3"));
    expected.put("io.netty:netty-codec", withUnverifiedRuntimeVersion("4.0.56.Final"));
    expected.put(
        "com.fasterxml.jackson.core:jackson-annotations", withUnverifiedRuntimeVersion("2.8.11"));
    expected.put("com.fasterxml.jackson.core:jackson-core", withUnverifiedRuntimeVersion("2.8.11"));
    expected.put("io.netty:netty-handler", withUnverifiedRuntimeVersion("4.0.56.Final"));
    expected.put("org.lz4:lz4-java", withUnverifiedRuntimeVersionOptional("1.4.1"));
    expected.put("org.hdrhistogram:HdrHistogram", withUnverifiedRuntimeVersionOptional("2.1.10"));
    expected.put("com.github.jnr:jffi", withUnverifiedRuntimeVersion("1.2.16"));
    expected.put("io.netty:netty-buffer", withUnverifiedRuntimeVersion("4.0.56.Final"));
    expected.put("org.ow2.asm:asm-commons", withUnverifiedRuntimeVersion("5.0.3"));
    expected.put("org.json:json", withUnverifiedRuntimeVersion("20090211"));
    expected.put("org.ow2.asm:asm-util", withUnverifiedRuntimeVersion("5.0.3"));
    expected.put("com.github.jnr:jnr-ffi", withUnverifiedRuntimeVersion("2.1.7"));

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::nullUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).hasSize(28);
    assertThat(stringStringMap).isEqualTo(expected);
  }

  @Test
  public void should_find_dependencies_from_file_without_duplicate() {
    // given
    InputStream inputStream =
        this.getClass().getResourceAsStream("/insights/duplicate-dependencies.txt");

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::nullUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).hasSize(1);
  }

  @Test
  public void should_keep_order_of_dependencies() {
    // given
    InputStream inputStream =
        this.getClass().getResourceAsStream("/insights/ordered-dependencies.txt");
    Map<String, RuntimeAndCompileTimeVersions> expected = new LinkedHashMap<>();
    expected.put("b-org.com:art1", withUnverifiedRuntimeVersion("1.0"));
    expected.put("a-org.com:art1", withUnverifiedRuntimeVersion("2.0"));
    expected.put("c-org.com:art1", withUnverifiedRuntimeVersion("3.0"));

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::nullUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).isEqualTo(expected);
    Iterator<String> iterator = expected.keySet().iterator();
    assertThat(iterator.next()).isEqualTo("b-org.com:art1");
    assertThat(iterator.next()).isEqualTo("a-org.com:art1");
    assertThat(iterator.next()).isEqualTo("c-org.com:art1");
  }

  @Test
  public void should_add_information_about_java_platform() {
    // given
    Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtimeDependencies = new HashMap<>();

    // when
    new PlatformInfoFinder(this::nullUrlProvider).addJavaVersion(runtimeDependencies);

    // then
    Map<String, RuntimeAndCompileTimeVersions> javaDependencies = runtimeDependencies.get("java");
    assertThat(javaDependencies.size()).isEqualTo(3);
  }

  @Test
  public void should_load_runtime_version_from_pom_properties_URL() {
    // given
    InputStream inputStream = this.getClass().getResourceAsStream("/insights/netty-dependency.txt");
    Map<String, RuntimeAndCompileTimeVersions> expected = new LinkedHashMap<>();
    expected.put(
        "io.netty:netty-handler",
        new RuntimeAndCompileTimeVersions("4.0.56.Final", "4.0.0.Final", false));

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::nettyUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).isEqualTo(expected);
  }

  @Test
  public void should_load_runtime_version_of_optional_dependency_from_pom_properties_URL() {
    // given
    InputStream inputStream =
        this.getClass().getResourceAsStream("/insights/netty-dependency-optional.txt");
    Map<String, RuntimeAndCompileTimeVersions> expected = new LinkedHashMap<>();
    expected.put(
        "io.netty:netty-handler",
        new RuntimeAndCompileTimeVersions("4.0.56.Final", "4.0.0.Final", true));

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::nettyUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).isEqualTo(expected);
  }

  @Test
  public void should_not_load_runtime_dependency_from_malformed_pom_properties() {
    // given
    InputStream inputStream = this.getClass().getResourceAsStream("/insights/netty-dependency.txt");
    Map<String, RuntimeAndCompileTimeVersions> expected = new LinkedHashMap<>();
    expected.put("io.netty:netty-handler", withUnverifiedRuntimeVersion("4.0.0.Final"));

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::malformedUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).isEqualTo(expected);
  }

  @Test
  public void should_not_load_runtime_dependency_from_non_existing_pom_properties() {
    // given
    InputStream inputStream = this.getClass().getResourceAsStream("/insights/netty-dependency.txt");
    Map<String, RuntimeAndCompileTimeVersions> expected = new LinkedHashMap<>();
    expected.put("io.netty:netty-handler", withUnverifiedRuntimeVersion("4.0.0.Final"));

    // when
    Map<String, RuntimeAndCompileTimeVersions> stringStringMap =
        new PlatformInfoFinder(this::nonExistingUrlProvider).fetchDependenciesFromFile(inputStream);

    // then
    assertThat(stringStringMap).isEqualTo(expected);
  }

  private RuntimeAndCompileTimeVersions withUnverifiedRuntimeVersion(String compileVersion) {
    return new RuntimeAndCompileTimeVersions(UNVERIFIED_RUNTIME_VERSION, compileVersion, false);
  }

  private RuntimeAndCompileTimeVersions withUnverifiedRuntimeVersionOptional(
      String compileVersion) {
    return new RuntimeAndCompileTimeVersions(UNVERIFIED_RUNTIME_VERSION, compileVersion, true);
  }
}
