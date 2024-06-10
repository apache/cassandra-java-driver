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
package com.datastax.oss.driver.core.config;

import static com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader.DEFAULT_CONFIG_SUPPLIER;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class DriverExecutionProfileReloadIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Before
  public void clearPrimes() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_periodically_reload_configuration() {
    String query = "mockquery";
    // Define a loader which configures a reload interval of 2s and current value of configSource.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString(
                        "basic.config-reload-interval = 2s\n"
                            + "basic.request.timeout = 2s\n"
                            + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()));
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .withConfigLoader(loader)
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .build()) {
      SIMULACRON_RULE.cluster().prime(when(query).then(noRows()).delay(4, TimeUnit.SECONDS));

      // Expect timeout since default session timeout is 2s
      try {
        session.execute(query);
        fail("DriverTimeoutException expected");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds and wait for config to reload.
      configSource.set("basic.request.timeout = 10s");
      waitForConfigChange(session, 3, TimeUnit.SECONDS);

      // Execute again, should not timeout.
      session.execute(query);
    }
  }

  @Test
  public void should_reload_configuration_when_event_fired() {
    String query = "mockquery";
    // Define a loader which configures no automatic reloads and current value of configSource.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString(
                        "basic.config-reload-interval = 0\n"
                            + "basic.request.timeout = 2s\n"
                            + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()));
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .withConfigLoader(loader)
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .build()) {
      SIMULACRON_RULE.cluster().prime(when(query).then(noRows()).delay(4, TimeUnit.SECONDS));

      // Expect timeout since default session timeout is 2s
      try {
        session.execute(query);
        fail("DriverTimeoutException expected");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds and trigger a manual reload.
      configSource.set("basic.request.timeout = 10s");
      session.getContext().getConfigLoader().reload();
      waitForConfigChange(session, 500, TimeUnit.MILLISECONDS);

      // Execute again, should not timeout.
      session.execute(query);
    }
  }

  @Test
  public void should_not_allow_dynamically_adding_profile() {
    String query = "mockquery";
    // Define a loader which configures a reload interval of 2s and current value of configSource.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString(
                        "basic.config-reload-interval = 2s\n" + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()));
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .withConfigLoader(loader)
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .build()) {
      SIMULACRON_RULE.cluster().prime(when(query).then(noRows()).delay(4, TimeUnit.SECONDS));

      // Expect failure because profile doesn't exist.
      try {
        session.execute(SimpleStatement.builder(query).setExecutionProfileName("slow").build());
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds on profile and wait for config to reload.
      configSource.set("profiles.slow.basic.request.timeout = 10s");
      waitForConfigChange(session, 3, TimeUnit.SECONDS);

      // Execute again, should expect to fail again because doesn't allow to dynamically define
      // profile.
      Throwable t =
          catchThrowable(
              () ->
                  session.execute(
                      SimpleStatement.builder(query).setExecutionProfileName("slow").build()));

      assertThat(t).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void should_reload_profile_config_when_reloading_config() {
    String query = "mockquery";
    // Define a loader which configures a reload interval of 2s and current value of configSource.
    // Define initial profile settings so it initially exists.
    AtomicReference<String> configSource = new AtomicReference<>("");
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () ->
                ConfigFactory.parseString(
                        "profiles.slow.basic.request.consistency = ONE\n"
                            + "basic.config-reload-interval = 2s\n"
                            + "basic.request.timeout = 2s\n"
                            + configSource.get())
                    .withFallback(DEFAULT_CONFIG_SUPPLIER.get()));
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .withConfigLoader(loader)
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .build()) {
      SIMULACRON_RULE.cluster().prime(when(query).then(noRows()).delay(4, TimeUnit.SECONDS));

      // Expect failure because profile doesn't exist.
      try {
        session.execute(SimpleStatement.builder(query).setExecutionProfileName("slow").build());
        fail("Expected DriverTimeoutException");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Bump up request timeout to 10 seconds on profile and wait for config to reload.
      configSource.set("profiles.slow.basic.request.timeout = 10s");
      waitForConfigChange(session, 3, TimeUnit.SECONDS);

      // Execute again, should succeed because profile timeout was increased.
      session.execute(SimpleStatement.builder(query).setExecutionProfileName("slow").build());
    }
  }

  private void waitForConfigChange(CqlSession session, long timeout, TimeUnit unit) {
    CountDownLatch latch = new CountDownLatch(1);
    ((InternalDriverContext) session.getContext())
        .getEventBus()
        .register(ConfigChangeEvent.class, (e) -> latch.countDown());
    try {
      boolean success = latch.await(timeout, unit);
      assertThat(success).isTrue();
    } catch (InterruptedException e) {
      throw new AssertionError("Interrupted while waiting for config change event", e);
    }
  }
}
