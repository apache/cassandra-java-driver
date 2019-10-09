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

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collection;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DataCentersFinderTest {

  @Test
  @UseDataProvider("hostProvider")
  public void should_detect_data_centers(
      int numberOfRemoteHosts,
      String dc1,
      NodeDistance h1Distance,
      String dc2,
      NodeDistance h2Distance,
      Set<String> expected) {
    // given
    DriverExecutionProfile executionProfile = mock(DriverExecutionProfile.class);
    when(executionProfile.getInt(CONNECTION_POOL_REMOTE_SIZE)).thenReturn(numberOfRemoteHosts);
    Collection<Node> nodes = mockNodes(dc1, h1Distance, dc2, h2Distance);

    // when
    Set<String> dataCenters = new DataCentersFinder().getDataCenters(nodes, executionProfile);

    // then
    assertThat(dataCenters).isEqualTo(Sets.newHashSet(expected));
  }

  @DataProvider
  public static Object[][] hostProvider() {
    return new Object[][] {
      {1, "dc1", NodeDistance.LOCAL, "dc2", NodeDistance.REMOTE, Sets.newHashSet("dc1", "dc2")},
      {1, "dc1", NodeDistance.LOCAL, "dc1", NodeDistance.REMOTE, Sets.newHashSet("dc1")},
      {0, "dc1", NodeDistance.LOCAL, "dc2", NodeDistance.REMOTE, Sets.newHashSet("dc1")},
      {0, "dc1", NodeDistance.IGNORED, "dc2", NodeDistance.REMOTE, Sets.newHashSet()},
      {1, "dc1", NodeDistance.IGNORED, "dc2", NodeDistance.REMOTE, Sets.newHashSet("dc2")},
      {1, "dc1", NodeDistance.LOCAL, "dc2", NodeDistance.IGNORED, Sets.newHashSet("dc1")},
      {0, "dc1", NodeDistance.IGNORED, "dc2", NodeDistance.REMOTE, Sets.newHashSet()},
      {0, "dc1", NodeDistance.LOCAL, "dc2", NodeDistance.IGNORED, Sets.newHashSet("dc1")},
    };
  }

  private Collection<Node> mockNodes(
      String dc1, NodeDistance h1Distance, String dc2, NodeDistance h2Distance) {
    Node n1 = mock(Node.class);
    when(n1.getDatacenter()).thenReturn(dc1);
    when(n1.getDistance()).thenReturn(h1Distance);

    Node n2 = mock(Node.class);
    when(n2.getDatacenter()).thenReturn(dc2);
    when(n2.getDistance()).thenReturn(h2Distance);

    return ImmutableSet.of(n1, n2);
  }
}
