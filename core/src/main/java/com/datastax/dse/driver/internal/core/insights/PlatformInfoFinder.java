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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.OS;
import static com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.RuntimeAndCompileTimeVersions;

import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.CPUS;
import com.datastax.oss.driver.internal.core.os.Native;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Pattern;

class PlatformInfoFinder {
  private static final String MAVEN_IGNORE_LINE = "The following files have been resolved:";
  private static final Pattern DEPENDENCY_SPLIT_REGEX = Pattern.compile(":");
  static final String UNVERIFIED_RUNTIME_VERSION = "UNVERIFIED";
  private final Function<DependencyFromFile, URL> propertiesUrlProvider;

  @SuppressWarnings("UnnecessaryLambda")
  private static final Function<DependencyFromFile, URL> M2_PROPERTIES_PROVIDER =
      d -> {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null) {
          contextClassLoader = PlatformInfoFinder.class.getClassLoader();
        }
        return contextClassLoader.getResource(
            "META-INF/maven/" + d.groupId + "/" + d.artifactId + "/pom.properties");
      };

  PlatformInfoFinder() {
    this(M2_PROPERTIES_PROVIDER);
  }

  @VisibleForTesting
  PlatformInfoFinder(Function<DependencyFromFile, URL> pomPropertiesUrlProvider) {
    this.propertiesUrlProvider = pomPropertiesUrlProvider;
  }

  InsightsPlatformInfo getInsightsPlatformInfo() {
    OS os = getOsInfo();
    CPUS cpus = getCpuInfo();
    Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtimeInfo = getRuntimeInfo();

    return new InsightsPlatformInfo(os, cpus, runtimeInfo);
  }

  private Map<String, Map<String, RuntimeAndCompileTimeVersions>> getRuntimeInfo() {
    Map<String, RuntimeAndCompileTimeVersions> coreDeps =
        fetchDependenciesFromFile(
            this.getClass().getResourceAsStream("/com/datastax/dse/driver/internal/deps.txt"));

    Map<String, RuntimeAndCompileTimeVersions> queryBuilderDeps =
        fetchDependenciesFromFile(
            this.getClass()
                .getResourceAsStream("/com/datastax/dse/driver/internal/querybuilder/deps.txt"));

    Map<String, RuntimeAndCompileTimeVersions> mapperProcessorDeps =
        fetchDependenciesFromFile(
            this.getClass()
                .getResourceAsStream(
                    "/com/datastax/dse/driver/internal/mapper/processor/deps.txt"));

    Map<String, RuntimeAndCompileTimeVersions> mapperRuntimeDeps =
        fetchDependenciesFromFile(
            this.getClass()
                .getResourceAsStream("/com/datastax/dse/driver/internal/mapper/deps.txt"));

    Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtimeDependencies =
        new LinkedHashMap<>();
    putIfNonEmpty(coreDeps, runtimeDependencies, "core");
    putIfNonEmpty(queryBuilderDeps, runtimeDependencies, "query-builder");
    putIfNonEmpty(mapperProcessorDeps, runtimeDependencies, "mapper-processor");
    putIfNonEmpty(mapperRuntimeDeps, runtimeDependencies, "mapper-runtime");
    addJavaVersion(runtimeDependencies);
    return runtimeDependencies;
  }

  private void putIfNonEmpty(
      Map<String, RuntimeAndCompileTimeVersions> moduleDependencies,
      Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtimeDependencies,
      String moduleName) {
    if (!moduleDependencies.isEmpty()) {
      runtimeDependencies.put(moduleName, moduleDependencies);
    }
  }

  @VisibleForTesting
  void addJavaVersion(Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtimeDependencies) {
    Package javaPackage = Runtime.class.getPackage();
    Map<String, RuntimeAndCompileTimeVersions> javaDependencies = new LinkedHashMap<>();
    javaDependencies.put(
        "version", toSameRuntimeAndCompileVersion(javaPackage.getImplementationVersion()));
    javaDependencies.put(
        "vendor", toSameRuntimeAndCompileVersion(javaPackage.getImplementationVendor()));
    javaDependencies.put(
        "title", toSameRuntimeAndCompileVersion(javaPackage.getImplementationTitle()));
    putIfNonEmpty(javaDependencies, runtimeDependencies, "java");
  }

  private RuntimeAndCompileTimeVersions toSameRuntimeAndCompileVersion(String version) {
    return new RuntimeAndCompileTimeVersions(version, version, false);
  }

  /**
   * Method is fetching dependencies from file. Lines in file should be in format:
   * com.organization:artifactId:jar:1.2.0 or com.organization:artifactId:jar:native:1.2.0
   *
   * <p>For such file the output will be: Map<String, RuntimeAndCompileTimeVersions>
   * "com.organization:artifactId",{"runtimeVersion":"1.2.0", "compileVersion:"1.2.0", "optional":
   * false} Duplicates will be omitted. If there are two dependencies for the exactly the same
   * organizationId:artifactId it is not deterministic which version will be taken. In the case of
   * an error while opening file this method will fail silently returning an empty Map
   */
  @VisibleForTesting
  Map<String, RuntimeAndCompileTimeVersions> fetchDependenciesFromFile(InputStream inputStream) {
    Map<String, RuntimeAndCompileTimeVersions> dependencies = new LinkedHashMap<>();
    if (inputStream == null) {
      return dependencies;
    }
    try {
      List<DependencyFromFile> dependenciesFromFile = extractMavenDependenciesFromFile(inputStream);
      for (DependencyFromFile d : dependenciesFromFile) {
        dependencies.put(formatDependencyName(d), getRuntimeAndCompileVersion(d));
      }
    } catch (IOException e) {
      return dependencies;
    }
    return dependencies;
  }

  private RuntimeAndCompileTimeVersions getRuntimeAndCompileVersion(DependencyFromFile d) {
    URL url = propertiesUrlProvider.apply(d);
    if (url == null) {
      return new RuntimeAndCompileTimeVersions(
          UNVERIFIED_RUNTIME_VERSION, d.getVersion(), d.isOptional());
    }
    Properties properties = new Properties();
    try {
      properties.load(url.openStream());
    } catch (IOException e) {
      return new RuntimeAndCompileTimeVersions(
          UNVERIFIED_RUNTIME_VERSION, d.getVersion(), d.isOptional());
    }
    Object version = properties.get("version");
    if (version == null) {
      return new RuntimeAndCompileTimeVersions(
          UNVERIFIED_RUNTIME_VERSION, d.getVersion(), d.isOptional());
    } else {
      return new RuntimeAndCompileTimeVersions(version.toString(), d.getVersion(), d.isOptional());
    }
  }

  private String formatDependencyName(DependencyFromFile d) {
    return String.format("%s:%s", d.getGroupId(), d.getArtifactId());
  }

  private List<DependencyFromFile> extractMavenDependenciesFromFile(InputStream inputStream)
      throws IOException {
    List<DependencyFromFile> dependenciesFromFile = new ArrayList<>();
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    for (String line; (line = reader.readLine()) != null; ) {
      if (lineWithDependencyInfo(line)) {
        dependenciesFromFile.add(extractDependencyFromLine(line.trim()));
      }
    }
    return dependenciesFromFile;
  }

  private DependencyFromFile extractDependencyFromLine(String line) {
    String[] split = DEPENDENCY_SPLIT_REGEX.split(line);
    if (split.length == 6) { // case for i.e.: com.github.jnr:jffi:jar:native:1.2.16:compile
      return new DependencyFromFile(split[0], split[1], split[4], checkIsOptional(split[5]));
    } else { // case for normal: org.ow2.asm:asm:jar:5.0.3:compile
      return new DependencyFromFile(split[0], split[1], split[3], checkIsOptional(split[4]));
    }
  }

  private boolean checkIsOptional(String scope) {
    return scope.contains("(optional)");
  }

  private boolean lineWithDependencyInfo(String line) {
    return (!line.equals(MAVEN_IGNORE_LINE) && !line.isEmpty());
  }

  private CPUS getCpuInfo() {
    int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    String model = Native.getCpu();
    return new CPUS(numberOfProcessors, model);
  }

  private OS getOsInfo() {
    String osName = System.getProperty("os.name");
    String osVersion = System.getProperty("os.version");
    String osArch = System.getProperty("os.arch");
    return new OS(osName, osVersion, osArch);
  }

  static class DependencyFromFile {
    private final String groupId;
    private final String artifactId;
    private final String version;
    private final boolean optional;

    DependencyFromFile(String groupId, String artifactId, String version, boolean optional) {
      this.groupId = groupId;
      this.artifactId = artifactId;
      this.version = version;
      this.optional = optional;
    }

    String getGroupId() {
      return groupId;
    }

    String getArtifactId() {
      return artifactId;
    }

    String getVersion() {
      return version;
    }

    boolean isOptional() {
      return optional;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DependencyFromFile)) {
        return false;
      }
      DependencyFromFile that = (DependencyFromFile) o;
      return optional == that.optional
          && Objects.equals(groupId, that.groupId)
          && Objects.equals(artifactId, that.artifactId)
          && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
      return Objects.hash(groupId, artifactId, version, optional);
    }

    @Override
    public String toString() {
      return "DependencyFromFile{"
          + "groupId='"
          + groupId
          + '\''
          + ", artifactId='"
          + artifactId
          + '\''
          + ", version='"
          + version
          + '\''
          + ", optional="
          + optional
          + '}';
    }
  }
}
