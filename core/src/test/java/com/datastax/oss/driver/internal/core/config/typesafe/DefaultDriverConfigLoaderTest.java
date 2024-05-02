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
package com.datastax.oss.driver.internal.core.config.typesafe;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.config.MockOptions;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop.CapturedTask;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.EventLoopGroup;
import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultDriverConfigLoaderTest {

  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private EventLoopGroup adminEventExecutorGroup;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;
  private ScheduledTaskCapturingEventLoop adminExecutor;
  private EventBus eventBus;
  private AtomicReference<String> configSource;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(context.getSessionName()).thenReturn("test");
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventExecutorGroup);

    adminExecutor = new ScheduledTaskCapturingEventLoop(adminEventExecutorGroup);
    when(adminEventExecutorGroup.next()).thenReturn(adminExecutor);

    eventBus = spy(new EventBus("test"));
    when(context.getEventBus()).thenReturn(eventBus);

    // The already loaded config in the context.
    // In real life, it's the object managed by the loader, but in this test it's simpler to mock
    // it.
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getDuration(DefaultDriverOption.CONFIG_RELOAD_INTERVAL))
        .thenReturn(Duration.ofSeconds(12));

    configSource = new AtomicReference<>("int1 = 42");
  }

  @Test
  public void should_build_initial_config() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);
  }

  @Test
  public void should_schedule_reloading_task() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    CapturedTask<?> task = adminExecutor.nextTask();
    assertThat(task.getInitialDelay(TimeUnit.SECONDS)).isEqualTo(12);
    assertThat(task.getPeriod(TimeUnit.SECONDS)).isEqualTo(12);
  }

  @Test
  public void should_detect_config_change_from_periodic_reload() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    CapturedTask<?> task = adminExecutor.nextTask();

    configSource.set("int1 = 43");

    task.run();

    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 43);
    verify(eventBus).fire(ConfigChangeEvent.INSTANCE);
  }

  @Test
  public void should_detect_config_change_from_manual_reload() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    configSource.set("int1 = 43");

    CompletionStage<Boolean> reloaded = loader.reload();
    adminExecutor.waitForNonScheduledTasks();

    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 43);
    verify(eventBus).fire(ConfigChangeEvent.INSTANCE);
    assertThatStage(reloaded).isSuccess(changed -> assertThat(changed).isTrue());
  }

  @Test
  public void should_not_notify_from_periodic_reload_if_config_has_not_changed() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    CapturedTask<?> task = adminExecutor.nextTask();

    // no change to the config source

    task.run();

    verify(eventBus, never()).fire(ConfigChangeEvent.INSTANCE);
  }

  @Test
  public void should_not_notify_from_manual_reload_if_config_has_not_changed() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    CompletionStage<Boolean> reloaded = loader.reload();
    adminExecutor.waitForNonScheduledTasks();

    verify(eventBus, never()).fire(ConfigChangeEvent.INSTANCE);
    assertThatStage(reloaded).isSuccess(changed -> assertThat(changed).isFalse());
  }

  @Test
  public void should_load_from_other_classpath_resource() {
    DriverConfigLoader loader = DriverConfigLoader.fromClasspath("config/customApplication");
    DriverExecutionProfile config = loader.getInitialConfig().getDefaultProfile();
    // From customApplication.conf:
    assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(5));
    // From customApplication.json:
    assertThat(config.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).isEqualTo(2000);
    // From customApplication.properties:
    assertThat(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.ONE.name());
    // From reference.conf:
    assertThat(config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
  }

  @Test
  public void should_load_from_file() {
    File file = new File("src/test/resources/config/customApplication.conf");
    assertThat(file).exists();
    DriverConfigLoader loader = DriverConfigLoader.fromFile(file);
    DriverExecutionProfile config = loader.getInitialConfig().getDefaultProfile();
    // From customApplication.conf:
    assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(5));
    // From reference.conf:
    assertThat(config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
  }

  @Test
  public void should_load_from_file_with_system_property() {
    File file = new File("src/test/resources/config/customApplication.conf");
    assertThat(file).exists();
    System.setProperty("config.file", file.getAbsolutePath());
    try {
      DriverConfigLoader loader = new DefaultDriverConfigLoader();
      DriverExecutionProfile config = loader.getInitialConfig().getDefaultProfile();
      // From customApplication.conf:
      assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
          .isEqualTo(Duration.ofSeconds(5));
      // From reference.conf:
      assertThat(config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
          .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
    } finally {
      System.clearProperty("config.file");
    }
  }

  @Test
  public void should_return_failed_future_if_reloading_not_supported() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()), false);
    assertThat(loader.supportsReloading()).isFalse();
    CompletionStage<Boolean> stage = loader.reload();
    assertThatStage(stage)
        .isFailed(
            t ->
                assertThat(t)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(
                        "This instance of DefaultDriverConfigLoader does not support reloading"));
  }

  /** Test for JAVA-2846. */
  @Test
  public void should_load_setting_from_system_property_when_application_conf_is_also_provided() {
    System.setProperty("datastax-java-driver.basic.request.timeout", "1 millisecond");
    try {
      assertThat(
              new DefaultDriverConfigLoader()
                  .getInitialConfig()
                  .getDefaultProfile()
                  .getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
          .isEqualTo(Duration.ofMillis(1));
    } finally {
      System.clearProperty("datastax-java-driver.basic.request.timeout");
    }
  }

  /** Test for JAVA-2846. */
  @Test
  public void
      should_load_and_resolve_setting_from_system_property_when_application_conf_is_also_provided() {
    System.setProperty(
        "datastax-java-driver.advanced.connection.init-query-timeout", "1234 milliseconds");
    try {
      assertThat(
              new DefaultDriverConfigLoader()
                  .getInitialConfig()
                  .getDefaultProfile()
                  .getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
          .isEqualTo(Duration.ofMillis(1234));
    } finally {
      System.clearProperty("datastax-java-driver.advanced.connection.init-query-timeout");
    }
  }

  /** Test for JAVA-2846. */
  @Test
  public void
      should_load_setting_from_system_property_when_application_conf_is_also_provided_for_custom_classloader() {
    System.setProperty("datastax-java-driver.basic.request.timeout", "1 millisecond");
    try {
      assertThat(
              new DefaultDriverConfigLoader(Thread.currentThread().getContextClassLoader())
                  .getInitialConfig()
                  .getDefaultProfile()
                  .getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
          .isEqualTo(Duration.ofMillis(1));
    } finally {
      System.clearProperty("datastax-java-driver.basic.request.timeout");
    }
  }

  @Test
  public void should_create_from_string() {
    DriverExecutionProfile config =
        DriverConfigLoader.fromString(
                "datastax-java-driver.basic { session-name = my-app\nrequest.timeout = 1 millisecond }")
            .getInitialConfig()
            .getDefaultProfile();

    assertThat(config.getString(DefaultDriverOption.SESSION_NAME)).isEqualTo("my-app");
    assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofMillis(1));
    // Any option not in the string should be pulled from reference.conf
    assertThat(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).isEqualTo("LOCAL_ONE");
  }
}
