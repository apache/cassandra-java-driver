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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.response.Supported;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
class HeartbeatHandler extends IdleStateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatHandler.class);

  private final DriverExecutionProfile config;

  private HeartbeatRequest request;

  HeartbeatHandler(DriverExecutionProfile config) {
    super((int) config.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL).getSeconds(), 0, 0);
    this.config = config;
  }

  @Override
  protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
    if (evt.state() == IdleState.READER_IDLE) {
      if (this.request != null) {
        LOG.warn(
            "Not sending heartbeat because a previous one is still in progress. "
                + "Check that {} is not lower than {}.",
            DefaultDriverOption.HEARTBEAT_INTERVAL.getPath(),
            DefaultDriverOption.HEARTBEAT_TIMEOUT.getPath());
      } else {
        LOG.debug(
            "Connection was inactive for {} seconds, sending heartbeat",
            config.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL).getSeconds());
        long timeoutMillis = config.getDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT).toMillis();
        this.request = new HeartbeatRequest(ctx, timeoutMillis);
        this.request.send();
      }
    }
  }

  private class HeartbeatRequest extends ChannelHandlerRequest {

    HeartbeatRequest(ChannelHandlerContext ctx, long timeoutMillis) {
      super(ctx, timeoutMillis);
    }

    @Override
    String describe() {
      return "Heartbeat request";
    }

    @Override
    Message getRequest() {
      return Options.INSTANCE;
    }

    @Override
    void onResponse(Message response) {
      if (response instanceof Supported) {
        LOG.debug("{} Heartbeat query succeeded", ctx.channel());
        HeartbeatHandler.this.request = null;
      } else {
        failOnUnexpected(response);
      }
    }

    @Override
    void fail(String message, Throwable cause) {
      if (cause instanceof HeartbeatException) {
        // Ignore: this happens when the heartbeat query times out and the inflight handler aborts
        // all queries (including the heartbeat query itself)
        return;
      }

      HeartbeatHandler.this.request = null;
      if (message != null) {
        LOG.debug("{} Heartbeat query failed: {}", ctx.channel(), message, cause);
      } else {
        LOG.debug("{} Heartbeat query failed", ctx.channel(), cause);
      }

      // Notify InFlightHandler.
      ctx.fireExceptionCaught(
          new HeartbeatException(ctx.channel().remoteAddress(), message, cause));
    }
  }
}
