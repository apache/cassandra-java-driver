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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.metadata.schema.events.AggregateChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.FunctionChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.ViewChangeEvent;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import io.netty.util.concurrent.EventExecutor;
import java.util.Set;

class SchemaListenerNotifier {

  private final Set<SchemaChangeListener> listeners;
  private final EventExecutor adminExecutor;

  SchemaListenerNotifier(
      Set<SchemaChangeListener> listeners, EventBus eventBus, EventExecutor adminExecutor) {
    this.listeners = listeners;
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

  private void onAggregateChangeEvent(AggregateChangeEvent event) {
    assert adminExecutor.inEventLoop();
    switch (event.changeType) {
      case CREATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onAggregateCreated(event.newAggregate);
        }
        break;
      case UPDATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onAggregateUpdated(event.newAggregate, event.oldAggregate);
        }
        break;
      case DROPPED:
        for (SchemaChangeListener listener : listeners) {
          listener.onAggregateDropped(event.oldAggregate);
        }
        break;
    }
  }

  private void onFunctionChangeEvent(FunctionChangeEvent event) {
    assert adminExecutor.inEventLoop();
    switch (event.changeType) {
      case CREATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onFunctionCreated(event.newFunction);
        }
        break;
      case UPDATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onFunctionUpdated(event.newFunction, event.oldFunction);
        }
        break;
      case DROPPED:
        for (SchemaChangeListener listener : listeners) {
          listener.onFunctionDropped(event.oldFunction);
        }
        break;
    }
  }

  private void onKeyspaceChangeEvent(KeyspaceChangeEvent event) {
    assert adminExecutor.inEventLoop();
    switch (event.changeType) {
      case CREATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onKeyspaceCreated(event.newKeyspace);
        }
        break;
      case UPDATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onKeyspaceUpdated(event.newKeyspace, event.oldKeyspace);
        }
        break;
      case DROPPED:
        for (SchemaChangeListener listener : listeners) {
          listener.onKeyspaceDropped(event.oldKeyspace);
        }
        break;
    }
  }

  private void onTableChangeEvent(TableChangeEvent event) {
    assert adminExecutor.inEventLoop();
    switch (event.changeType) {
      case CREATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onTableCreated(event.newTable);
        }
        break;
      case UPDATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onTableUpdated(event.newTable, event.oldTable);
        }
        break;
      case DROPPED:
        for (SchemaChangeListener listener : listeners) {
          listener.onTableDropped(event.oldTable);
        }
        break;
    }
  }

  private void onTypeChangeEvent(TypeChangeEvent event) {
    assert adminExecutor.inEventLoop();
    switch (event.changeType) {
      case CREATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onUserDefinedTypeCreated(event.newType);
        }
        break;
      case UPDATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onUserDefinedTypeUpdated(event.newType, event.oldType);
        }
        break;
      case DROPPED:
        for (SchemaChangeListener listener : listeners) {
          listener.onUserDefinedTypeDropped(event.oldType);
        }
        break;
    }
  }

  private void onViewChangeEvent(ViewChangeEvent event) {
    assert adminExecutor.inEventLoop();
    switch (event.changeType) {
      case CREATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onViewCreated(event.newView);
        }
        break;
      case UPDATED:
        for (SchemaChangeListener listener : listeners) {
          listener.onViewUpdated(event.newView, event.oldView);
        }
        break;
      case DROPPED:
        for (SchemaChangeListener listener : listeners) {
          listener.onViewDropped(event.oldView);
        }
        break;
    }
  }
}
