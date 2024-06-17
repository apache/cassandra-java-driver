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

import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class OutboundTrafficMeter extends ChannelOutboundHandlerAdapter {

  private final NodeMetricUpdater nodeMetricUpdater;
  private final SessionMetricUpdater sessionMetricUpdater;

  OutboundTrafficMeter(
      NodeMetricUpdater nodeMetricUpdater, SessionMetricUpdater sessionMetricUpdater) {
    this.nodeMetricUpdater = nodeMetricUpdater;
    this.sessionMetricUpdater = sessionMetricUpdater;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof ByteBuf) {
      int bytes = ((ByteBuf) msg).readableBytes();
      nodeMetricUpdater.markMeter(DefaultNodeMetric.BYTES_SENT, null, bytes);
      sessionMetricUpdater.markMeter(DefaultSessionMetric.BYTES_SENT, null, bytes);
    }
    super.write(ctx, msg, promise);
  }
}
