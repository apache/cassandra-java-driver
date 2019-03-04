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
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;

/**
 * Low-level SSL extension point.
 *
 * <p>SSL is separated into two interfaces to avoid exposing Netty classes in our public API:
 *
 * <ul>
 *   <li>"normal" (JDK-based) SSL is part of the public API, and can be configured via an instance
 *       of {@link com.datastax.oss.driver.api.core.ssl.SslEngineFactory} defined in the driver
 *       configuration.
 *   <li>this interface deals with Netty handlers directly. It can be used for more advanced cases,
 *       like using Netty's native OpenSSL integration instead of the JDK. This is considered expert
 *       level, and therefore part of our internal API.
 * </ul>
 *
 * @see DefaultDriverContext#buildSslHandlerFactory()
 */
public interface SslHandlerFactory extends AutoCloseable {
  SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint);
}
