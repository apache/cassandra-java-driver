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
package com.datastax.oss.driver.api.core.config;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.LongTests;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.config.ForceReloadConfigEvent;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader.DEFAULT_CONFIG_SUPPLIER;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Category(LongTests.class)
public class DriverConfigProfileReloadIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void should_periodically_reload_configuration() throws Exception {
    String query = "mockquery";
    // Define a loader which configures a reload interval of 2s and current value of configSource.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString("config-reload-interval = 2s\n" + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()),
            CoreDriverOption.values());
    try (Cluster<CqlSession> configCluster =
        Cluster.builder()
            .withConfigLoader(loader)
            .addContactPoints(simulacron.getContactPoints())
            .build()) {
      simulacron.cluster().prime(when(query).then(noRows()).delay(2, TimeUnit.SECONDS));

      CqlSession session = configCluster.connect();

      // Expect timeout since default timeout is .5 s
      try {
        session.execute(query);
        fail("DriverTimeoutException expected");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds and wait for config to reload.
      configSource.set("request.timeout = 10s");
      waitForConfigChange(configCluster, 3, TimeUnit.SECONDS);

      // Execute again, should not timeout.
      session.execute(query);
    }
  }

  @Test
  public void should_reload_configuration_when_event_fired() throws Exception {
    String query = "mockquery";
    // Define a loader which configures no automatic reloads and current value of configSource.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString("config-reload-interval = 0\n" + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()),
            CoreDriverOption.values());
    try (Cluster<CqlSession> configCluster =
        Cluster.builder()
            .withConfigLoader(loader)
            .addContactPoints(simulacron.getContactPoints())
            .build()) {
      simulacron.cluster().prime(when(query).then(noRows()).delay(2, TimeUnit.SECONDS));

      CqlSession session = configCluster.connect();

      // Expect timeout since default timeout is .5 s
      try {
        session.execute(query);
        fail("DriverTimeoutException expected");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds and trigger a manual reload.
      configSource.set("request.timeout = 10s");
      ((InternalDriverContext) configCluster.getContext())
          .eventBus()
          .fire(ForceReloadConfigEvent.INSTANCE);
      waitForConfigChange(configCluster, 500, TimeUnit.MILLISECONDS);

      // Execute again, should not timeout.
      session.execute(query);
    }
  }

  @Test
  public void should_not_allow_dynamically_adding_profile() throws Exception {
    String query = "mockquery";
    // Define a loader which configures a reload interval of 2s and current value of configSource.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString("config-reload-interval = 2s\n" + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()),
            CoreDriverOption.values());
    try (Cluster<CqlSession> configCluster =
        Cluster.builder()
            .withConfigLoader(loader)
            .addContactPoints(simulacron.getContactPoints())
            .build()) {
      simulacron.cluster().prime(when(query).then(noRows()).delay(1, TimeUnit.SECONDS));

      CqlSession session = configCluster.connect();

      // Expect failure because profile doesn't exist.
      try {
        session.execute(SimpleStatement.builder(query).withConfigProfileName("slow").build());
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds on profile and wait for config to reload.
      configSource.set("profiles.slow.request.timeout = 2s");
      waitForConfigChange(configCluster, 3, TimeUnit.SECONDS);

      // Execute again, should expect to fail again because doesn't allow to dynamically define profile.
      thrown.expect(IllegalArgumentException.class);
      session.execute(SimpleStatement.builder(query).withConfigProfileName("slow").build());
    }
  }

  @Test
  public void should_reload_profile_config_when_reloading_config() throws Exception {
    String query = "mockquery";
    // Define a loader which configures a reload interval of 2s and current value of configSource.
    // Define initial profile settings so it initially exists.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString(
                        "profiles.slow.request.consistency = ONE\nconfig-reload-interval = 2s\n"
                            + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()),
            CoreDriverOption.values());
    try (Cluster<CqlSession> configCluster =
        Cluster.builder()
            .withConfigLoader(loader)
            .addContactPoints(simulacron.getContactPoints())
            .build()) {
      simulacron.cluster().prime(when(query).then(noRows()).delay(1, TimeUnit.SECONDS));

      CqlSession session = configCluster.connect();

      // Expect failure because profile doesn't exist.
      try {
        session.execute(SimpleStatement.builder(query).withConfigProfileName("slow").build());
        fail("Expected DriverTimeoutException");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds on profile and wait for config to reload.
      configSource.set("profiles.slow.request.timeout = 10s");
      waitForConfigChange(configCluster, 3, TimeUnit.SECONDS);

      // Execute again, should succeed because profile timeout was increased.
      session.execute(SimpleStatement.builder(query).withConfigProfileName("slow").build());
    }
  }

  private void waitForConfigChange(Cluster<CqlSession> cluster, long timeout, TimeUnit unit) {
    CountDownLatch latch = new CountDownLatch(1);
    ((InternalDriverContext) cluster.getContext())
        .eventBus()
        .register(ConfigChangeEvent.class, (e) -> latch.countDown());
    try {
      boolean success = latch.await(timeout, unit);
      assertThat(success).isTrue();
    } catch (InterruptedException e) {
      fail("Interrupted while waiting for config change event");
    }
  }
}
