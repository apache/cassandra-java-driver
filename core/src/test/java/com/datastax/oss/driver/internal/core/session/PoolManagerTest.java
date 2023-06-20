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
package com.datastax.oss.driver.internal.core.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import io.netty.channel.DefaultEventLoopGroup;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PoolManagerTest {
  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    DefaultEventLoopGroup adminEventLoopGroup = new DefaultEventLoopGroup(1);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(context.getEventBus()).thenReturn(new EventBus("test"));
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(context.getConfig()).thenReturn(config);
  }

  @Test
  public void should_use_weak_values_if_config_is_true_or_undefined() {
    when(defaultProfile.getBoolean(DefaultDriverOption.PREPARED_CACHE_WEAK_VALUES, true))
        .thenReturn(true);
    // As weak values map class is MapMakerInternalMap
    assertThat(new PoolManager(context).getRepreparePayloads())
        .isNotInstanceOf(ConcurrentHashMap.class);
  }

  @Test
  public void should_not_use_weak_values_if_config_is_false() {
    when(defaultProfile.getBoolean(DefaultDriverOption.PREPARED_CACHE_WEAK_VALUES, true))
        .thenReturn(false);
    assertThat(new PoolManager(context).getRepreparePayloads())
        .isInstanceOf(ConcurrentHashMap.class);
  }
}
