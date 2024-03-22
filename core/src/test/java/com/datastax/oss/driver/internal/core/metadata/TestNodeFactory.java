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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.net.InetSocketAddress;
import java.util.UUID;

public class TestNodeFactory {

  public static DefaultNode newNode(int lastIpByte, InternalDriverContext context) {
    DefaultNode node = newContactPoint(lastIpByte, context);
    node.hostId = UUID.randomUUID();
    node.broadcastRpcAddress = ((InetSocketAddress) node.getEndPoint().retrieve());
    return node;
  }

  public static DefaultNode newNode(int lastIpByte, UUID hostId, InternalDriverContext context) {
    DefaultNode node = newContactPoint(lastIpByte, context);
    node.hostId = hostId;
    node.broadcastRpcAddress = ((InetSocketAddress) node.getEndPoint().retrieve());
    return node;
  }

  public static DefaultNode newContactPoint(int lastIpByte, InternalDriverContext context) {
    DefaultEndPoint endPoint = newEndPoint(lastIpByte);
    return new DefaultNode(endPoint, context);
  }

  public static DefaultEndPoint newEndPoint(int lastByteOfIp) {
    return new DefaultEndPoint(new InetSocketAddress("127.0.0." + lastByteOfIp, 9042));
  }
}
