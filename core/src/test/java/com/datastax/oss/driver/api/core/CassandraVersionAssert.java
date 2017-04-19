/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core;

import org.assertj.core.api.AbstractComparableAssert;
import org.assertj.core.api.Assertions;

import static com.datastax.oss.driver.Assertions.assertThat;

public class CassandraVersionAssert
    extends AbstractComparableAssert<CassandraVersionAssert, CassandraVersion> {

  public CassandraVersionAssert(CassandraVersion actual) {
    super(actual, CassandraVersionAssert.class);
  }

  public CassandraVersionAssert hasMajorMinorPatch(int major, int minor, int patch) {
    Assertions.assertThat(actual.getMajor()).isEqualTo(major);
    Assertions.assertThat(actual.getMinor()).isEqualTo(minor);
    Assertions.assertThat(actual.getPatch()).isEqualTo(patch);
    return this;
  }

  public CassandraVersionAssert hasDsePatch(int dsePatch) {
    Assertions.assertThat(actual.getDSEPatch()).isEqualTo(dsePatch);
    return this;
  }

  public CassandraVersionAssert hasPreReleaseLabels(String... labels) {
    Assertions.assertThat(actual.getPreReleaseLabels()).containsExactly(labels);
    return this;
  }

  public CassandraVersionAssert hasNoPreReleaseLabels() {
    Assertions.assertThat(actual.getPreReleaseLabels()).isNull();
    return this;
  }

  public CassandraVersionAssert hasBuildLabel(String label) {
    Assertions.assertThat(actual.getBuildLabel()).isEqualTo(label);
    return this;
  }

  public CassandraVersionAssert hasNextStable(String version) {
    assertThat(actual.nextStable()).isEqualTo(CassandraVersion.parse(version));
    return this;
  }

  public CassandraVersionAssert hasToString(String string) {
    Assertions.assertThat(actual.toString()).isEqualTo(string);
    return this;
  }
}
