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
package com.datastax.oss.driver.api.core;

import static com.datastax.oss.driver.Assertions.assertThat;

import org.assertj.core.api.AbstractComparableAssert;

public class VersionAssert extends AbstractComparableAssert<VersionAssert, Version> {

  public VersionAssert(Version actual) {
    super(actual, VersionAssert.class);
  }

  public VersionAssert hasMajorMinorPatch(int major, int minor, int patch) {
    assertThat(actual.getMajor()).isEqualTo(major);
    assertThat(actual.getMinor()).isEqualTo(minor);
    assertThat(actual.getPatch()).isEqualTo(patch);
    return this;
  }

  public VersionAssert hasDsePatch(int dsePatch) {
    assertThat(actual.getDSEPatch()).isEqualTo(dsePatch);
    return this;
  }

  public VersionAssert hasPreReleaseLabels(String... labels) {
    assertThat(actual.getPreReleaseLabels()).containsExactly(labels);
    return this;
  }

  public VersionAssert hasNoPreReleaseLabels() {
    assertThat(actual.getPreReleaseLabels()).isNull();
    return this;
  }

  public VersionAssert hasBuildLabel(String label) {
    assertThat(actual.getBuildLabel()).isEqualTo(label);
    return this;
  }

  public VersionAssert hasNextStable(String version) {
    assertThat(actual.nextStable()).isEqualTo(Version.parse(version));
    return this;
  }

  @Override
  public VersionAssert hasToString(String string) {
    assertThat(actual.toString()).isEqualTo(string);
    return this;
  }
}
