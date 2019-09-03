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

import static org.assertj.core.api.Assertions.fail;

import java.net.InetSocketAddress;
import org.scassandra.Scassandra;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.CurrentClient;
import org.scassandra.http.client.PrimingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Base class for Scassandra tests. This class takes care of starting and stopping a Scassandra
 * server, and provides some helper methods to leverage the creation of Cluster and Session objects.
 * The actual creation of such objects is however left to subclasses. If a single cluster instance
 * can be shared by all test methods, consider using {@link
 * com.datastax.driver.core.ScassandraTestBase.PerClassCluster} instead.
 */
public abstract class ScassandraTestBase {

  private static final Logger logger = LoggerFactory.getLogger(ScassandraTestBase.class);

  protected Scassandra scassandra;

  protected EndPoint hostEndPoint;

  protected PrimingClient primingClient;

  protected ActivityClient activityClient;

  protected CurrentClient currentClient;

  protected static String ip = TestUtils.ipOfNode(1);

  @BeforeClass(groups = {"short", "long"})
  public void beforeTestClass() {
    scassandra = TestUtils.createScassandraServer();
    scassandra.start();
    primingClient = scassandra.primingClient();
    activityClient = scassandra.activityClient();
    currentClient = scassandra.currentClient();
    hostEndPoint =
        new TranslatedAddressEndPoint(new InetSocketAddress(ip, scassandra.getBinaryPort()));
  }

  @AfterClass(groups = {"short", "long"})
  public void afterTestClass() {
    if (scassandra != null) {
      try {
        scassandra.stop();
      } catch (Exception e) {
        logger.error("Could not stop node " + scassandra, e);
      }
    }
  }

  @BeforeMethod(groups = {"short", "long"})
  @AfterMethod(groups = {"short", "long"})
  public void resetClients() {
    activityClient.clearAllRecordedActivity();
    primingClient.clearAllPrimes();
    currentClient.enableListener();
    ScassandraCluster.primeSystemLocalRow(scassandra);
  }

  protected Cluster.Builder createClusterBuilder() {
    return Cluster.builder()
        .withPort(scassandra.getBinaryPort())
        .addContactPoint(hostEndPoint)
        .withPort(scassandra.getBinaryPort())
        .withPoolingOptions(
            new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                .setHeartbeatIntervalSeconds(0));
  }

  protected Host retrieveSingleHost(Cluster cluster) {
    Host host = cluster.getMetadata().getHost(hostEndPoint);
    if (host == null) {
      fail("Unable to retrieve host");
    }
    return host;
  }

  /**
   * This subclass of ScassandraTestBase assumes that the same Cluster instance will be used for all
   * tests.
   */
  public abstract static class PerClassCluster extends ScassandraTestBase {

    protected Cluster cluster;

    protected Session session;

    protected Host host;

    @Override
    @BeforeClass(groups = {"short", "long"})
    public void beforeTestClass() {
      super.beforeTestClass();
      ScassandraCluster.primeSystemLocalRow(scassandra);
      Cluster.Builder builder = createClusterBuilder();
      cluster = builder.build();
      host = retrieveSingleHost(cluster);
      session = cluster.connect();
    }

    @Override
    @AfterClass(groups = {"short", "long"})
    public void afterTestClass() {
      if (cluster != null) cluster.close();
      super.afterTestClass();
    }
  }
}
