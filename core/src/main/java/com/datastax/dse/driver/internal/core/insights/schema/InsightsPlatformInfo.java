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
package com.datastax.dse.driver.internal.core.insights.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

public class InsightsPlatformInfo {
  @JsonProperty("os")
  private final OS os;

  @JsonProperty("cpus")
  private CPUS cpus;

  /**
   * All dependencies in a map format grouped by the module: {"core" : {"com.datastax.driver:core":
   * {"runtimeVersion:" : "1.0.0", "compileVersion": "1.0.1"},...}}, "extras"" {...}
   */
  @JsonProperty("runtime")
  private Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtime;

  @JsonCreator
  public InsightsPlatformInfo(
      @JsonProperty("os") OS os,
      @JsonProperty("cpus") CPUS cpus,
      @JsonProperty("runtime") Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtime) {
    this.os = os;
    this.cpus = cpus;
    this.runtime = runtime;
  }

  public OS getOs() {
    return os;
  }

  public CPUS getCpus() {
    return cpus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InsightsPlatformInfo)) {
      return false;
    }
    InsightsPlatformInfo that = (InsightsPlatformInfo) o;
    return Objects.equals(os, that.os)
        && Objects.equals(cpus, that.cpus)
        && Objects.equals(runtime, that.runtime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(os, cpus, runtime);
  }

  Map<String, Map<String, RuntimeAndCompileTimeVersions>> getRuntime() {
    return runtime;
  }

  public static class OS {
    @JsonProperty("name")
    private final String name;

    @JsonProperty("version")
    private final String version;

    @JsonProperty("arch")
    private final String arch;

    @JsonCreator
    public OS(
        @JsonProperty("name") String name,
        @JsonProperty("version") String version,
        @JsonProperty("arch") String arch) {
      this.name = name;
      this.version = version;
      this.arch = arch;
    }

    public String getName() {
      return name;
    }

    public String getVersion() {
      return version;
    }

    public String getArch() {
      return arch;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OS)) {
        return false;
      }
      OS os = (OS) o;
      return Objects.equals(name, os.name)
          && Objects.equals(version, os.version)
          && Objects.equals(arch, os.arch);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, version, arch);
    }
  }

  public static class CPUS {
    @JsonProperty("length")
    private final int length;

    @JsonProperty("model")
    private final String model;

    @JsonCreator
    public CPUS(@JsonProperty("length") int length, @JsonProperty("model") String model) {
      this.length = length;
      this.model = model;
    }

    public int getLength() {
      return length;
    }

    public String getModel() {
      return model;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CPUS)) {
        return false;
      }
      CPUS cpus = (CPUS) o;
      return length == cpus.length && Objects.equals(model, cpus.model);
    }

    @Override
    public int hashCode() {
      return Objects.hash(length, model);
    }
  }

  public static class RuntimeAndCompileTimeVersions {
    @JsonProperty("runtimeVersion")
    private final String runtimeVersion;

    @JsonProperty("compileVersion")
    private final String compileVersion;

    @JsonProperty("optional")
    private final boolean optional;

    @JsonCreator
    public RuntimeAndCompileTimeVersions(
        @JsonProperty("runtimeVersion") String runtimeVersion,
        @JsonProperty("compileVersion") String compileVersion,
        @JsonProperty("optional") boolean optional) {
      this.runtimeVersion = runtimeVersion;
      this.compileVersion = compileVersion;
      this.optional = optional;
    }

    public String getRuntimeVersion() {
      return runtimeVersion;
    }

    public String getCompileVersion() {
      return compileVersion;
    }

    public boolean isOptional() {
      return optional;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RuntimeAndCompileTimeVersions)) {
        return false;
      }
      RuntimeAndCompileTimeVersions that = (RuntimeAndCompileTimeVersions) o;
      return optional == that.optional
          && Objects.equals(runtimeVersion, that.runtimeVersion)
          && Objects.equals(compileVersion, that.compileVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(runtimeVersion, compileVersion, optional);
    }

    @Override
    public String toString() {
      return "RuntimeAndCompileTimeVersions{"
          + "runtimeVersion='"
          + runtimeVersion
          + '\''
          + ", compileVersion='"
          + compileVersion
          + '\''
          + ", optional="
          + optional
          + '}';
    }
  }
}
