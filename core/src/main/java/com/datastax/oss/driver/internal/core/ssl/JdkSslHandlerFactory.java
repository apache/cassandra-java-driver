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
package com.datastax.oss.driver.internal.core.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import javax.net.ssl.SSLEngine;
import net.jcip.annotations.ThreadSafe;

/** SSL handler factory used when JDK-based SSL was configured through the driver's public API. */
@ThreadSafe
public class JdkSslHandlerFactory implements SslHandlerFactory {
  private final SslEngineFactory sslEngineFactory;

  public JdkSslHandlerFactory(SslEngineFactory sslEngineFactory) {
    this.sslEngineFactory = sslEngineFactory;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
    SSLEngine engine = sslEngineFactory.newSslEngine(remoteEndpoint);
    return new SslHandler(engine);
  }

  @Override
  public void close() throws Exception {
    sslEngineFactory.close();
  }
}
