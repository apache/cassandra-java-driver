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
package com.datastax.driver.core;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.Iterators;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import org.testng.annotations.Test;

// Test that PoolingOpions.refreshConnectedHosts works as expected (JAVA-309)
@CreateCCM(PER_METHOD)
public class LoadBalancingPolicyRefreshTest extends CCMTestsSupport {

  UpdatablePolicy policy;

  private class UpdatablePolicy implements LoadBalancingPolicy {

    private Cluster cluster;
    private Host theHost;

    public void changeTheHost(Host theNewHost) {
      this.theHost = theNewHost;
      cluster.getConfiguration().getPoolingOptions().refreshConnectedHosts();
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
      this.cluster = cluster;
      try {
        for (Host h : hosts)
          if (h.getEndPoint()
              .resolve()
              .getAddress()
              .equals(InetAddress.getByName(TestUtils.IP_PREFIX + '1'))) this.theHost = h;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public HostDistance distance(Host host) {
      return host == theHost ? HostDistance.LOCAL : HostDistance.IGNORED;
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
      return Iterators.<Host>singletonIterator(theHost);
    }

    @Override
    public void onAdd(Host h) {}

    @Override
    public void onRemove(Host h) {}

    @Override
    public void onUp(Host h) {}

    @Override
    public void onDown(Host h) {}

    @Override
    public void close() {}
  }

  @Test(groups = "short")
  @CCMConfig(numberOfNodes = 2, clusterProvider = "updatablePolicy")
  public void refreshTest() throws Throwable {
    // Ugly
    Host[] hosts = new Host[2];
    for (Host h : cluster().getMetadata().getAllHosts()) {
      if (h.getEndPoint().resolve().equals(ccm().addressOfNode(1))) hosts[0] = h;
      else hosts[1] = h;
    }

    assertTrue(
        session().getState().getConnectedHosts().contains(hosts[0]),
        "Connected hosts: " + session().getState().getConnectedHosts());
    assertTrue(
        !session().getState().getConnectedHosts().contains(hosts[1]),
        "Connected hosts: " + session().getState().getConnectedHosts());

    policy.changeTheHost(hosts[1]);

    assertTrue(
        !session().getState().getConnectedHosts().contains(hosts[0]),
        "Connected hosts: " + session().getState().getConnectedHosts());
    assertTrue(
        session().getState().getConnectedHosts().contains(hosts[1]),
        "Connected hosts: " + session().getState().getConnectedHosts());
  }

  @SuppressWarnings("unused")
  private Cluster.Builder updatablePolicy() {
    policy = new UpdatablePolicy();
    return Cluster.builder().withLoadBalancingPolicy(policy);
  }
}
