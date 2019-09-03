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

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.HostFilterPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Predicate;
import org.mockito.Mockito;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HostTargetingTest {

  private ScassandraCluster sCluster;
  private Cluster cluster;
  private Session session;
  // Allow connecting to all hosts except for the 4th one.
  private LoadBalancingPolicy lbSpy =
      Mockito.spy(
          new HostFilterPolicy(
              DCAwareRoundRobinPolicy.builder().build(),
              new Predicate<Host>() {
                @Override
                public boolean apply(Host host) {
                  return !host.getEndPoint().resolve().getAddress().getHostAddress().endsWith("4");
                }
              }));

  @BeforeMethod(groups = "short")
  public void setUp() {
    sCluster = ScassandraCluster.builder().withNodes(4).build();
    sCluster.init();

    cluster =
        Cluster.builder()
            .addContactPoints(sCluster.address(1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(lbSpy)
            .withNettyOptions(nonQuietClusterCloseOptions)
            .build();
    session = cluster.connect();

    // Reset invocations before entering test.
    Mockito.reset(lbSpy);
  }

  @AfterMethod(groups = "short")
  public void tearDown() {
    if (cluster != null) {
      cluster.close();
    }
    if (sCluster != null) {
      sCluster.stop();
    }
  }

  private void verifyNoLbpInteractions() {
    // load balancing policy should have been skipped completely as host was set.
    Mockito.verify(lbSpy, Mockito.times(0))
        .newQueryPlan(Mockito.any(String.class), Mockito.any(Statement.class));
  }

  @Test(groups = "short")
  public void should_use_host_on_statement() {
    for (int i = 0; i < 10; i++) {
      int hostIndex = i % 3 + 1;
      Host host = TestUtils.findHost(cluster, hostIndex);

      // given a statement with host explicitly set.
      Statement statement = new SimpleStatement("select * system.local").setHost(host);

      // when statement is executed
      ResultSet result = session.execute(statement);

      // then the query should have been sent to the configured host.
      assertThat(result.getExecutionInfo().getQueriedHost()).isSameAs(host);

      verifyNoLbpInteractions();
    }
  }

  @Test(groups = "short")
  public void should_fail_if_host_fails_query() {
    String query = "mock";
    sCluster
        .node(1)
        .primingClient()
        .prime(
            PrimingRequest.queryBuilder()
                .withQuery(query)
                .withThen(then().withResult(Result.unavailable))
                .build());

    // given a statement with a host configured to fail the given query.
    Host host1 = TestUtils.findHost(cluster, 1);
    Statement statement = new SimpleStatement(query).setHost(host1);

    try {
      // when statement is executed an error should be raised.
      session.execute(statement);
      fail("Query should have failed");
    } catch (NoHostAvailableException e) {
      // then the request should fail with a NHAE and no host was tried.
      assertThat(e.getErrors()).hasSize(1);
      assertThat(e.getErrors().values().iterator().next()).isInstanceOf(UnavailableException.class);
    } finally {
      verifyNoLbpInteractions();
    }
  }

  @Test(groups = "short")
  public void should_fail_if_host_is_not_connected() {
    // given a statement with host explicitly set that for which we have no active pool.
    Host host4 = TestUtils.findHost(cluster, 4);
    Statement statement = new SimpleStatement("select * system.local").setHost(host4);

    try {
      // when statement is executed
      session.execute(statement);
      fail("Query should have failed");
    } catch (NoHostAvailableException e) {
      // then the request should fail with a NHAE and no host was tried.
      assertThat(e.getErrors()).isEmpty();
    } finally {
      verifyNoLbpInteractions();
    }
  }
}
