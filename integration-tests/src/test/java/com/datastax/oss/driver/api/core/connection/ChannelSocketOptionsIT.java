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
package com.datastax.oss.driver.api.core.connection;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SOCKET_KEEP_ALIVE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SOCKET_LINGER_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SOCKET_REUSE_ADDRESS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SOCKET_TCP_NODELAY;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.SocketChannelConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ChannelSocketOptionsIT {

  public static @ClassRule SimulacronRule simulacron =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static DriverConfigLoader loader =
      SessionUtils.configLoaderBuilder()
          .withBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY, true)
          .withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, false)
          .withBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS, false)
          .withInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL, 10)
          .withInt(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, 123456)
          .withInt(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE, 123456)
          .build();

  @ClassRule
  public static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(simulacron).withConfigLoader(loader).build();

  @Test
  public void should_report_socket_options() {
    CqlSession session = sessionRule.session();
    DriverExecutionProfile config = session.getContext().getConfig().getDefaultProfile();
    assertThat(config.getBoolean(SOCKET_TCP_NODELAY)).isTrue();
    assertThat(config.getBoolean(SOCKET_KEEP_ALIVE)).isFalse();
    assertThat(config.getBoolean(SOCKET_REUSE_ADDRESS)).isFalse();
    assertThat(config.getInt(SOCKET_LINGER_INTERVAL)).isEqualTo(10);
    assertThat(config.getInt(SOCKET_RECEIVE_BUFFER_SIZE)).isEqualTo(123456);
    assertThat(config.getInt(SOCKET_SEND_BUFFER_SIZE)).isEqualTo(123456);
    Node node = session.getMetadata().getNodes().values().iterator().next();
    DriverChannel channel = ((DefaultSession) session).getChannel(node, null);
    assertThat(channel.config()).isInstanceOf(SocketChannelConfig.class);
    SocketChannelConfig socketConfig = (SocketChannelConfig) channel.config();
    assertThat(socketConfig.isTcpNoDelay()).isTrue();
    assertThat(socketConfig.isKeepAlive()).isFalse();
    assertThat(socketConfig.isReuseAddress()).isFalse();
    assertThat(socketConfig.getSoLinger()).isEqualTo(10);
    RecvByteBufAllocator allocator = socketConfig.getRecvByteBufAllocator();
    assertThat(allocator).isInstanceOf(FixedRecvByteBufAllocator.class);
    assertThat(allocator.newHandle().guess()).isEqualTo(123456);
    // cannot assert around SO_RCVBUF and SO_SNDBUF, such values are just hints
  }
}
