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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import net.jcip.annotations.GuardedBy;

/**
 * A node state listener wrapper that delays (or ignores) init events until after the session is
 * ready.
 *
 * <p>By default, the driver calls node state events, such as {@link #onUp} and {@link #onAdd},
 * before the session is ready; see {@link NodeStateListener#onSessionReady(Session)} for a detailed
 * explanation. This can make things complicated if your listener implementation needs the session
 * to process those events.
 *
 * <p>This class wraps another implementation to shield it from those details:
 *
 * <pre>
 * NodeStateListener delegate = ... // your listener implementation
 *
 * SafeInitNodeStateListener wrapper =
 *     new SafeInitNodeStateListener(delegate, true);
 *
 * CqlSession session = CqlSession.builder()
 *     .withNodeStateListener(wrapper)
 *     .build();
 * </pre>
 *
 * With this setup, {@code delegate.onSessionReady} is guaranteed to be invoked first, before any
 * other method. The second constructor argument indicates what to do with the method calls that
 * were ignored before that:
 *
 * <ul>
 *   <li>if {@code true}, they are recorded, and replayed to {@code delegate} immediately after
 *       {@link #onSessionReady}. They are guaranteed to happen in the original order, and before
 *       any post-initialization events.
 *   <li>if {@code false}, they are discarded.
 * </ul>
 *
 * <p>Usage in non-blocking applications: beware that this class is not lock-free. It is implemented
 * with locks for internal coordination.
 *
 * @since 4.6.0
 */
public class SafeInitNodeStateListener implements NodeStateListener {

  private final NodeStateListener delegate;
  private final boolean replayInitEvents;

  // Write lock: recording init events or setting sessionReady
  // Read lock: reading init events or checking sessionReady
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  @GuardedBy("lock")
  private boolean sessionReady;

  @GuardedBy("lock")
  private final List<InitEvent> initEvents = new ArrayList<>();

  /**
   * Creates a new instance.
   *
   * @param delegate the wrapped listener, to which method invocations will be forwarded.
   * @param replayInitEvents whether to record events during initialization and replay them to the
   *     child listener once it's created, or just ignore them.
   */
  public SafeInitNodeStateListener(@NonNull NodeStateListener delegate, boolean replayInitEvents) {
    this.delegate = Objects.requireNonNull(delegate);
    this.replayInitEvents = replayInitEvents;
  }

  @Override
  public void onSessionReady(@NonNull Session session) {
    lock.writeLock().lock();
    try {
      if (!sessionReady) {
        sessionReady = true;
        delegate.onSessionReady(session);
        if (replayInitEvents) {
          for (InitEvent event : initEvents) {
            event.invoke(delegate);
          }
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void onAdd(@NonNull Node node) {
    onEvent(node, InitEvent.Type.ADD);
  }

  @Override
  public void onUp(@NonNull Node node) {
    onEvent(node, InitEvent.Type.UP);
  }

  @Override
  public void onDown(@NonNull Node node) {
    onEvent(node, InitEvent.Type.DOWN);
  }

  @Override
  public void onRemove(@NonNull Node node) {
    onEvent(node, InitEvent.Type.REMOVE);
  }

  private void onEvent(Node node, InitEvent.Type eventType) {

    // Cheap case: the session is ready, just delegate
    lock.readLock().lock();
    try {
      if (sessionReady) {
        eventType.listenerMethod.accept(delegate, node);
        return;
      }
    } finally {
      lock.readLock().unlock();
    }

    // Otherwise, we must acquire the write lock to record the event
    if (replayInitEvents) {
      lock.writeLock().lock();
      try {
        // Must re-check because we completely released the lock for a short duration
        if (sessionReady) {
          eventType.listenerMethod.accept(delegate, node);
        } else {
          initEvents.add(new InitEvent(node, eventType));
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  private static class InitEvent {
    enum Type {
      ADD(NodeStateListener::onAdd),
      UP(NodeStateListener::onUp),
      DOWN(NodeStateListener::onDown),
      REMOVE(NodeStateListener::onRemove),
      ;

      @SuppressWarnings("ImmutableEnumChecker")
      final BiConsumer<NodeStateListener, Node> listenerMethod;

      Type(BiConsumer<NodeStateListener, Node> listenerMethod) {
        this.listenerMethod = listenerMethod;
      }
    }

    final Node node;
    final Type type;

    InitEvent(@NonNull Node node, @NonNull Type type) {
      this.node = Objects.requireNonNull(node);
      this.type = Objects.requireNonNull(type);
    }

    void invoke(@NonNull NodeStateListener target) {
      type.listenerMethod.accept(Objects.requireNonNull(target), node);
    }
  }
}
