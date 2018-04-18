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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.Version;
import java.net.URL;
import org.junit.Test;

public class DefaultDriverInfoTest {

  @Test
  public void should_report_name() {
    // given
    DefaultDriverInfo info = new DefaultDriverInfo("My Name", null, null, null);
    // when
    String name = info.getName();
    // then
    assertThat(name).isEqualTo("My Name");
  }

  @Test
  public void should_report_groupId() {
    // given
    DefaultDriverInfo info = new DefaultDriverInfo(null, "My GroupId", null, null);
    // when
    String groupId = info.getGroupId();
    // then
    assertThat(groupId).isEqualTo("My GroupId");
  }

  @Test
  public void should_report_artifactId() {
    // given
    DefaultDriverInfo info = new DefaultDriverInfo(null, null, "My ArtifactId", null);
    // when
    String artifactId = info.getArtifactId();
    // then
    assertThat(artifactId).isEqualTo("My ArtifactId");
  }

  @Test
  public void should_report_version() {
    // given
    DefaultDriverInfo info = new DefaultDriverInfo(null, null, null, Version.parse("1.2.3"));
    // when
    Version version = info.getVersion();
    // then
    assertThat(version).isEqualTo(Version.parse("1.2.3"));
  }

  // Note: when ran from IntelliJ this test may fail because IntelliJ by default does not filter
  // resources when building
  @Test
  public void should_build_from_resource() {
    // given
    URL resource = getClass().getResource("/com/datastax/oss/driver/Driver.properties");
    // when
    DefaultDriverInfo info = DefaultDriverInfo.buildFromResource(resource);
    // then
    assertThat(info).isNotNull();
  }

  @Test
  public void should_return_all_info_in_toString() {
    // given
    DefaultDriverInfo info =
        new DefaultDriverInfo(
            "My Driver Name", "com.datastax.foo", "java-driver-core", Version.parse("1.2.3"));
    // when
    String s = info.toString();
    // then
    assertThat(s).isEqualTo("My Driver Name (com.datastax.foo:java-driver-core) version 1.2.3");
  }
}
