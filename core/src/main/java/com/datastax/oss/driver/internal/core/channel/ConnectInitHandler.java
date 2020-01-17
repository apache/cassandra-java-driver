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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.internal.core.util.concurrent.PromiseCombiner;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import net.jcip.annotations.NotThreadSafe;

/**
 * A handler that delays the promise returned by {@code bootstrap.connect()}, in order to run a
 * custom initialization process before making the channel available to clients.
 *
 * <p>This handler is not shareable. It must be installed by the channel initializer, as the last
 * channel in the pipeline.
 *
 * <p>It will be notified via {@link #onRealConnect(ChannelHandlerContext)} when the real underlying
 * connection is established. It can then start sending messages on the connection, while external
 * clients are still waiting on their promise. Once the custom initialization is finished, the
 * clients' promise can be completed with {@link #setConnectSuccess()} or {@link
 * #setConnectFailure(Throwable)}.
 */
@NotThreadSafe
public abstract class ConnectInitHandler extends ChannelDuplexHandler {
  // the completion of the custom initialization process
  private ChannelPromise initPromise;
  private ChannelHandlerContext ctx;

  @Override
  public void connect(
      ChannelHandlerContext ctx,
      SocketAddress remoteAddress,
      SocketAddress localAddress,
      ChannelPromise callerPromise)
      throws Exception {
    this.ctx = ctx;
    initPromise = ctx.channel().newPromise();

    // the completion of the real underlying connection:
    ChannelPromise realConnectPromise = ctx.channel().newPromise();
    super.connect(ctx, remoteAddress, localAddress, realConnectPromise);
    realConnectPromise.addListener(future -> onRealConnect(ctx));

    // Make the caller's promise wait on the other two:
    PromiseCombiner.combine(callerPromise, realConnectPromise, initPromise);
  }

  protected abstract void onRealConnect(ChannelHandlerContext ctx);

  protected boolean setConnectSuccess() {
    boolean result = initPromise.trySuccess();
    if (result) {
      ctx.pipeline().remove(this);
    }
    return result;
  }

  protected void setConnectFailure(Throwable cause) {
    if (initPromise.tryFailure(cause)) {
      ctx.channel().close();
    }
  }
}
