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

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import net.jcip.annotations.ThreadSafe;

/**
 * Default node state listener implementation with empty methods.
 *
 * <p>To activate this listener, modify the {@code advanced.node-state-listener} section in the
 * driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.node-state-listener {
 *     class = NoopNodeStateListener
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p>Note that if a listener is specified programmatically with {@link
 * SessionBuilder#withNodeStateListener(NodeStateListener)}, the configuration is ignored.
 */
@ThreadSafe
public class NoopNodeStateListener extends NodeStateListenerBase {

  public NoopNodeStateListener(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }
}
