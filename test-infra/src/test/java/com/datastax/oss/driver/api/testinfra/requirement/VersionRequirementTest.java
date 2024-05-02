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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class VersionRequirementTest {
  // backend aliases
  private static BackendType CASSANDRA = BackendType.CASSANDRA;
  private static BackendType DSE = BackendType.DSE;

  // version numbers
  private static Version V_0_0_0 = Version.parse("0.0.0");
  private static Version V_0_1_0 = Version.parse("0.1.0");
  private static Version V_1_0_0 = Version.parse("1.0.0");
  private static Version V_1_0_1 = Version.parse("1.0.1");
  private static Version V_1_1_0 = Version.parse("1.1.0");
  private static Version V_2_0_0 = Version.parse("2.0.0");
  private static Version V_2_0_1 = Version.parse("2.0.1");
  private static Version V_3_0_0 = Version.parse("3.0.0");
  private static Version V_3_1_0 = Version.parse("3.1.0");
  private static Version V_4_0_0 = Version.parse("4.0.0");

  // requirements
  private static VersionRequirement CASSANDRA_ANY = new VersionRequirement(CASSANDRA, "", "", "");
  private static VersionRequirement CASSANDRA_FROM_1_0_0 =
      new VersionRequirement(CASSANDRA, "1.0.0", "", "");
  private static VersionRequirement CASSANDRA_TO_1_0_0 =
      new VersionRequirement(CASSANDRA, "", "1.0.0", "");
  private static VersionRequirement CASSANDRA_FROM_1_0_0_TO_2_0_0 =
      new VersionRequirement(CASSANDRA, "1.0.0", "2.0.0", "");
  private static VersionRequirement CASSANDRA_FROM_1_1_0 =
      new VersionRequirement(CASSANDRA, "1.1.0", "", "");
  private static VersionRequirement CASSANDRA_FROM_3_0_0_TO_3_1_0 =
      new VersionRequirement(CASSANDRA, "3.0.0", "3.1.0", "");
  private static VersionRequirement DSE_ANY = new VersionRequirement(DSE, "", "", "");

  @Test
  public void empty_requirements() {
    List<VersionRequirement> req = Collections.emptyList();

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, DSE, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isTrue();
  }

  @Test
  public void single_requirement_any_version() {
    List<VersionRequirement> anyCassandra = Collections.singletonList(CASSANDRA_ANY);
    List<VersionRequirement> anyDse = Collections.singletonList(DSE_ANY);

    assertThat(VersionRequirement.meetsAny(anyCassandra, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(anyCassandra, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(anyDse, DSE, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(anyDse, DSE, V_1_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(anyDse, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(anyDse, CASSANDRA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(anyCassandra, DSE, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(anyCassandra, DSE, V_1_0_0)).isFalse();
  }

  @Test
  public void single_requirement_min_only() {
    List<VersionRequirement> req = Collections.singletonList(CASSANDRA_FROM_1_0_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_1_0)).isFalse();
  }

  @Test
  public void single_requirement_max_only() {
    List<VersionRequirement> req = Collections.singletonList(CASSANDRA_TO_1_0_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_1_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, DSE, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isFalse();
  }

  @Test
  public void single_requirement_min_and_max() {
    List<VersionRequirement> req = Collections.singletonList(CASSANDRA_FROM_1_0_0_TO_2_0_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_1_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_1)).isFalse();
  }

  @Test
  public void multi_requirement_any_version() {
    List<VersionRequirement> req = ImmutableList.of(CASSANDRA_ANY, DSE_ANY);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isTrue();
  }

  @Test
  public void multi_db_requirement_min_one_any_other() {
    List<VersionRequirement> req = ImmutableList.of(CASSANDRA_FROM_1_0_0, DSE_ANY);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, DSE, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
  }

  @Test
  public void multi_requirement_two_ranges() {
    List<VersionRequirement> req =
        ImmutableList.of(CASSANDRA_FROM_1_0_0_TO_2_0_0, CASSANDRA_FROM_3_0_0_TO_3_1_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_3_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_3_1_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_4_0_0)).isFalse();
  }

  @Test
  public void multi_requirement_overlapping() {
    List<VersionRequirement> req =
        ImmutableList.of(CASSANDRA_FROM_1_0_0_TO_2_0_0, CASSANDRA_FROM_1_1_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, DSE, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
  }

  @Test
  public void multi_requirement_not_range() {
    List<VersionRequirement> req = ImmutableList.of(CASSANDRA_TO_1_0_0, CASSANDRA_FROM_1_1_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isFalse();
  }
}
