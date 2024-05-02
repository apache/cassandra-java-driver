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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.metadata.schema.events.AggregateChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.FunctionChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.ViewChangeEvent;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import io.netty.util.concurrent.EventExecutor;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
class SchemaListenerNotifier {

  private final SchemaChangeListener listener;
  private final EventExecutor adminExecutor;

  // It is technically possible that a schema change could happen in the middle of session
  // initialization. Don't forward events in this case, it would likely do more harm than good if a
  // listener implementation doesn't expect it.
  private boolean sessionReady;

  SchemaListenerNotifier(
      SchemaChangeListener listener, EventBus eventBus, EventExecutor adminExecutor) {
    this.listener = listener;
    this.adminExecutor = adminExecutor;

    // No need to unregister at shutdown, this component has the same lifecycle as the cluster
    eventBus.register(
        AggregateChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onAggregateChangeEvent));
    eventBus.register(
        FunctionChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onFunctionChangeEvent));
    eventBus.register(
        KeyspaceChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onKeyspaceChangeEvent));
    eventBus.register(
        TableChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onTableChangeEvent));
    eventBus.register(
        TypeChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onTypeChangeEvent));
    eventBus.register(
        ViewChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onViewChangeEvent));
  }

  void onSessionReady(Session session) {
    RunOrSchedule.on(
        adminExecutor,
        () -> {
          sessionReady = true;
          listener.onSessionReady(session);
        });
  }

  private void onAggregateChangeEvent(AggregateChangeEvent event) {
    assert adminExecutor.inEventLoop();
    if (sessionReady) {
      switch (event.changeType) {
        case CREATED:
          listener.onAggregateCreated(event.newAggregate);
          break;
        case UPDATED:
          listener.onAggregateUpdated(event.newAggregate, event.oldAggregate);
          break;
        case DROPPED:
          listener.onAggregateDropped(event.oldAggregate);
          break;
      }
    }
  }

  private void onFunctionChangeEvent(FunctionChangeEvent event) {
    assert adminExecutor.inEventLoop();
    if (sessionReady) {
      switch (event.changeType) {
        case CREATED:
          listener.onFunctionCreated(event.newFunction);
          break;
        case UPDATED:
          listener.onFunctionUpdated(event.newFunction, event.oldFunction);
          break;
        case DROPPED:
          listener.onFunctionDropped(event.oldFunction);
          break;
      }
    }
  }

  private void onKeyspaceChangeEvent(KeyspaceChangeEvent event) {
    assert adminExecutor.inEventLoop();
    if (sessionReady) {
      switch (event.changeType) {
        case CREATED:
          listener.onKeyspaceCreated(event.newKeyspace);
          break;
        case UPDATED:
          listener.onKeyspaceUpdated(event.newKeyspace, event.oldKeyspace);
          break;
        case DROPPED:
          listener.onKeyspaceDropped(event.oldKeyspace);
          break;
      }
    }
  }

  private void onTableChangeEvent(TableChangeEvent event) {
    assert adminExecutor.inEventLoop();
    if (sessionReady) {
      switch (event.changeType) {
        case CREATED:
          listener.onTableCreated(event.newTable);
          break;
        case UPDATED:
          listener.onTableUpdated(event.newTable, event.oldTable);
          break;
        case DROPPED:
          listener.onTableDropped(event.oldTable);
          break;
      }
    }
  }

  private void onTypeChangeEvent(TypeChangeEvent event) {
    assert adminExecutor.inEventLoop();
    if (sessionReady) {
      switch (event.changeType) {
        case CREATED:
          listener.onUserDefinedTypeCreated(event.newType);
          break;
        case UPDATED:
          listener.onUserDefinedTypeUpdated(event.newType, event.oldType);
          break;
        case DROPPED:
          listener.onUserDefinedTypeDropped(event.oldType);
          break;
      }
    }
  }

  private void onViewChangeEvent(ViewChangeEvent event) {
    assert adminExecutor.inEventLoop();
    if (sessionReady) {
      switch (event.changeType) {
        case CREATED:
          listener.onViewCreated(event.newView);
          break;
        case UPDATED:
          listener.onViewUpdated(event.newView, event.oldView);
          break;
        case DROPPED:
          listener.onViewDropped(event.oldView);
          break;
      }
    }
  }
}
