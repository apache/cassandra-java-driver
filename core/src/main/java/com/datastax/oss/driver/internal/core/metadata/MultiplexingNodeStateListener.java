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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.Loggers;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combines multiple node state listeners into a single one.
 *
 * <p>Any exception thrown by a child listener is caught and logged.
 */
@ThreadSafe
public class MultiplexingNodeStateListener implements NodeStateListener {

  private static final Logger LOG = LoggerFactory.getLogger(MultiplexingNodeStateListener.class);

  private final List<NodeStateListener> listeners = new CopyOnWriteArrayList<>();

  public MultiplexingNodeStateListener() {}

  public MultiplexingNodeStateListener(NodeStateListener... listeners) {
    this(Arrays.asList(listeners));
  }

  public MultiplexingNodeStateListener(Collection<NodeStateListener> listeners) {
    addListeners(listeners);
  }

  private void addListeners(Collection<NodeStateListener> source) {
    for (NodeStateListener listener : source) {
      addListener(listener);
    }
  }

  private void addListener(NodeStateListener toAdd) {
    Objects.requireNonNull(toAdd, "listener cannot be null");
    if (toAdd instanceof MultiplexingNodeStateListener) {
      addListeners(((MultiplexingNodeStateListener) toAdd).listeners);
    } else {
      listeners.add(toAdd);
    }
  }

  public void register(@NonNull NodeStateListener listener) {
    addListener(listener);
  }

  @Override
  public void onAdd(@NonNull Node node) {
    invokeListeners(listener -> listener.onAdd(node), "onAdd");
  }

  @Override
  public void onUp(@NonNull Node node) {
    invokeListeners(listener -> listener.onUp(node), "onUp");
  }

  @Override
  public void onDown(@NonNull Node node) {
    invokeListeners(listener -> listener.onDown(node), "onDown");
  }

  @Override
  public void onRemove(@NonNull Node node) {
    invokeListeners(listener -> listener.onRemove(node), "onRemove");
  }

  @Override
  public void onSessionReady(@NonNull Session session) {
    invokeListeners(listener -> listener.onSessionReady(session), "onSessionReady");
  }

  @Override
  public void close() throws Exception {
    for (NodeStateListener listener : listeners) {
      try {
        listener.close();
      } catch (Exception e) {
        Loggers.warnWithException(
            LOG, "Unexpected error while closing node state listener {}.", listener, e);
      }
    }
  }

  private void invokeListeners(@NonNull Consumer<NodeStateListener> action, String event) {
    for (NodeStateListener listener : listeners) {
      try {
        action.accept(listener);
      } catch (Exception e) {
        Loggers.warnWithException(
            LOG,
            "Unexpected error while notifying node state listener {} of an {} event.",
            listener,
            event,
            e);
      }
    }
  }
}
