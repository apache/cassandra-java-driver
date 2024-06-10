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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collection;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class InsightsSupportVerifierTest {

  @Test
  @UseDataProvider(value = "dseHostsProvider")
  public void should_detect_DSE_versions_that_supports_insights(
      Collection<Node> hosts, boolean expected) {
    // when
    boolean result = InsightsSupportVerifier.supportsInsights(hosts);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @DataProvider
  public static Object[][] dseHostsProvider() {
    Node dse605 = mock(Node.class);
    when(dse605.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("6.0.5")));
    Node dse604 = mock(Node.class);
    when(dse604.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("6.0.4")));
    Node dse600 = mock(Node.class);
    when(dse600.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("6.0.0")));
    Node dse5113 = mock(Node.class);
    when(dse5113.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("5.1.13")));
    Node dse500 = mock(Node.class);
    when(dse500.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("5.0.0")));
    Node nodeWithoutExtras = mock(Node.class);
    when(nodeWithoutExtras.getExtras()).thenReturn(Collections.emptyMap());

    return new Object[][] {
      {ImmutableList.of(dse605), true},
      {ImmutableList.of(dse604), false},
      {ImmutableList.of(dse600), false},
      {ImmutableList.of(dse5113), true},
      {ImmutableList.of(dse500), false},
      {ImmutableList.of(dse5113, dse605), true},
      {ImmutableList.of(dse5113, dse600), false},
      {ImmutableList.of(dse500, dse600), false},
      {ImmutableList.of(), false},
      {ImmutableList.of(nodeWithoutExtras), false}
    };
  }
}
