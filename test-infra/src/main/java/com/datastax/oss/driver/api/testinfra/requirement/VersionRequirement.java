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
package com.datastax.oss.driver.api.testinfra.requirement;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.runner.Description;

/**
 * Used to unify the requirements specified by
 * annotations @CassandraRequirement, @DseRequirment, @BackendRequirement
 */
public class VersionRequirement {
  final BackendType backendType;
  final Optional<Version> minInclusive;
  final Optional<Version> maxExclusive;
  final String description;

  public VersionRequirement(
      BackendType backendType, String minInclusive, String maxExclusive, String description) {
    this.backendType = backendType;
    this.minInclusive =
        minInclusive.isEmpty() ? Optional.empty() : Optional.of(Version.parse(minInclusive));
    this.maxExclusive =
        maxExclusive.isEmpty() ? Optional.empty() : Optional.of(Version.parse(maxExclusive));
    this.description = description;
  }

  public BackendType getBackendType() {
    return backendType;
  }

  public Optional<Version> getMinInclusive() {
    return minInclusive;
  }

  public Optional<Version> getMaxExclusive() {
    return maxExclusive;
  }

  public String readableString() {
    final String versionRange;
    if (minInclusive.isPresent() && maxExclusive.isPresent()) {
      versionRange =
          String.format("%s or greater, but less than %s", minInclusive.get(), maxExclusive.get());
    } else if (minInclusive.isPresent()) {
      versionRange = String.format("%s or greater", minInclusive.get());
    } else if (maxExclusive.isPresent()) {
      versionRange = String.format("less than %s", maxExclusive.get());
    } else {
      versionRange = "any version";
    }

    if (!description.isEmpty()) {
      return String.format("%s %s [%s]", backendType.getFriendlyName(), versionRange, description);
    } else {
      return String.format("%s %s", backendType.getFriendlyName(), versionRange);
    }
  }

  public static VersionRequirement fromBackendRequirement(BackendRequirement requirement) {
    return new VersionRequirement(
        requirement.type(),
        requirement.minInclusive(),
        requirement.maxExclusive(),
        requirement.description());
  }

  public static VersionRequirement fromCassandraRequirement(CassandraRequirement requirement) {
    return new VersionRequirement(
        BackendType.CASSANDRA, requirement.min(), requirement.max(), requirement.description());
  }

  public static VersionRequirement fromDseRequirement(DseRequirement requirement) {
    return new VersionRequirement(
        BackendType.DSE, requirement.min(), requirement.max(), requirement.description());
  }

  public static Collection<VersionRequirement> fromAnnotations(Description description) {
    // collect all requirement annotation types
    CassandraRequirement cassandraRequirement =
        description.getAnnotation(CassandraRequirement.class);
    DseRequirement dseRequirement = description.getAnnotation(DseRequirement.class);
    // matches methods/classes with one @BackendRequirement annotation
    BackendRequirement backendRequirement = description.getAnnotation(BackendRequirement.class);
    // matches methods/classes with two or more @BackendRequirement annotations
    BackendRequirements backendRequirements = description.getAnnotation(BackendRequirements.class);

    // build list of required versions
    Collection<VersionRequirement> requirements = new ArrayList<>();
    if (cassandraRequirement != null) {
      requirements.add(VersionRequirement.fromCassandraRequirement(cassandraRequirement));
    }
    if (dseRequirement != null) {
      requirements.add(VersionRequirement.fromDseRequirement(dseRequirement));
    }
    if (backendRequirement != null) {
      requirements.add(VersionRequirement.fromBackendRequirement(backendRequirement));
    }
    if (backendRequirements != null) {
      Arrays.stream(backendRequirements.value())
          .forEach(r -> requirements.add(VersionRequirement.fromBackendRequirement(r)));
    }
    return requirements;
  }

  public static boolean meetsAny(
      Collection<VersionRequirement> requirements,
      BackendType configuredBackend,
      Version configuredVersion) {
    // special case: if there are no requirements then any backend/version is sufficient
    if (requirements.isEmpty()) {
      return true;
    }

    return requirements.stream()
        .anyMatch(
            requirement -> {
              // requirement is different db type
              if (requirement.getBackendType() != configuredBackend) {
                return false;
              }

              // configured version is less than requirement min
              if (requirement.getMinInclusive().isPresent()) {
                if (requirement.getMinInclusive().get().compareTo(configuredVersion) > 0) {
                  return false;
                }
              }

              // configured version is greater than or equal to requirement max
              if (requirement.getMaxExclusive().isPresent()) {
                if (requirement.getMaxExclusive().get().compareTo(configuredVersion) <= 0) {
                  return false;
                }
              }

              // backend type and version range match
              return true;
            });
  }

  public static String buildReasonString(
      Collection<VersionRequirement> requirements, BackendType backend, Version version) {
    return String.format(
        "Test requires one of:\n%s\nbut configuration is %s %s.",
        requirements.stream()
            .map(req -> String.format("  - %s", req.readableString()))
            .collect(Collectors.joining("\n")),
        backend.getFriendlyName(),
        version);
  }
}
