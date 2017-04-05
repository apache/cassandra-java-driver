/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import org.assertj.core.api.AbstractComparableAssert;

import static com.datastax.driver.core.Assertions.assertThat;

public class VersionNumberAssert extends AbstractComparableAssert<VersionNumberAssert, VersionNumber> {

    public VersionNumberAssert(VersionNumber actual) {
        super(actual, VersionNumberAssert.class);
    }

    public VersionNumberAssert hasMajorMinorPatch(int major, int minor, int patch) {
        assertThat(actual.getMajor()).isEqualTo(major);
        assertThat(actual.getMinor()).isEqualTo(minor);
        assertThat(actual.getPatch()).isEqualTo(patch);
        return this;
    }

    public VersionNumberAssert hasDsePatch(int dsePatch) {
        assertThat(actual.getDSEPatch()).isEqualTo(dsePatch);
        return this;
    }

    public VersionNumberAssert hasPreReleaseLabels(String... labels) {
        assertThat(actual.getPreReleaseLabels()).containsExactly(labels);
        return this;
    }

    public VersionNumberAssert hasNoPreReleaseLabels() {
        assertThat(actual.getPreReleaseLabels()).isNull();
        return this;
    }

    public VersionNumberAssert hasBuildLabel(String label) {
        assertThat(actual.getBuildLabel()).isEqualTo(label);
        return this;
    }

    public VersionNumberAssert hasNextStable(String version) {
        assertThat(actual.nextStable()).isEqualTo(VersionNumber.parse(version));
        return this;
    }

    public VersionNumberAssert hasToString(String string) {
        assertThat(actual.toString()).isEqualTo(string);
        return this;
    }
}
