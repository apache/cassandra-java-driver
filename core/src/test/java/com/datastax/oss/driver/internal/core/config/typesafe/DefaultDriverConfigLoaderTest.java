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
package com.datastax.oss.driver.internal.core.config.typesafe;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.config.ForceReloadConfigEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop.CapturedTask;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class DefaultDriverConfigLoaderTest {

  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private EventLoopGroup adminEventExecutorGroup;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfigProfile;
  private ScheduledTaskCapturingEventLoop adminExecutor;
  private EventBus eventBus;
  private AtomicReference<String> configSource;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(context.sessionName()).thenReturn("test");
    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventExecutorGroup);

    adminExecutor = new ScheduledTaskCapturingEventLoop(adminEventExecutorGroup);
    Mockito.when(adminEventExecutorGroup.next()).thenReturn(adminExecutor);

    eventBus = Mockito.spy(new EventBus("test"));
    Mockito.when(context.eventBus()).thenReturn(eventBus);

    // The already loaded config in the context.
    // In real life, it's the object managed by the loader, but in this test it's simpler to mock
    // it.
    Mockito.when(context.config()).thenReturn(config);
    Mockito.when(config.getDefaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(defaultConfigProfile.getDuration(DefaultDriverOption.CONFIG_RELOAD_INTERVAL))
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
  public void should_reload_if_config_has_changed() {
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
    Mockito.verify(eventBus).fire(ConfigChangeEvent.INSTANCE);
  }

  @Test
  public void should_reload_if_forced() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    configSource.set("int1 = 43");

    eventBus.fire(ForceReloadConfigEvent.INSTANCE);
    adminExecutor.waitForNonScheduledTasks();

    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 43);
    Mockito.verify(eventBus).fire(ConfigChangeEvent.INSTANCE);
  }

  @Test
  public void should_not_notify_if_config_has_not_changed() {
    DefaultDriverConfigLoader loader =
        new DefaultDriverConfigLoader(() -> ConfigFactory.parseString(configSource.get()));
    DriverConfig initialConfig = loader.getInitialConfig();
    assertThat(initialConfig).hasIntOption(MockOptions.INT1, 42);

    loader.onDriverInit(context);
    adminExecutor.waitForNonScheduledTasks();

    CapturedTask<?> task = adminExecutor.nextTask();

    // no change to the config source

    task.run();

    Mockito.verify(eventBus, never()).fire(ConfigChangeEvent.INSTANCE);
  }
}
