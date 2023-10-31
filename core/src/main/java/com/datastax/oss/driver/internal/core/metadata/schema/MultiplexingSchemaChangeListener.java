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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.util.Loggers;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combines multiple schema change listeners into a single one.
 *
 * <p>Any exception thrown by a child listener is caught and logged.
 */
@ThreadSafe
public class MultiplexingSchemaChangeListener implements SchemaChangeListener {

  private static final Logger LOG = LoggerFactory.getLogger(MultiplexingSchemaChangeListener.class);

  private final List<SchemaChangeListener> listeners = new CopyOnWriteArrayList<>();

  public MultiplexingSchemaChangeListener() {}

  public MultiplexingSchemaChangeListener(SchemaChangeListener... listeners) {
    this(Arrays.asList(listeners));
  }

  public MultiplexingSchemaChangeListener(Collection<SchemaChangeListener> listeners) {
    addListeners(listeners);
  }

  private void addListeners(Collection<SchemaChangeListener> source) {
    for (SchemaChangeListener listener : source) {
      addListener(listener);
    }
  }

  private void addListener(SchemaChangeListener toAdd) {
    Objects.requireNonNull(toAdd, "listener cannot be null");
    if (toAdd instanceof MultiplexingSchemaChangeListener) {
      addListeners(((MultiplexingSchemaChangeListener) toAdd).listeners);
    } else {
      listeners.add(toAdd);
    }
  }

  public void register(@Nonnull SchemaChangeListener listener) {
    addListener(listener);
  }

  @Override
  public void onKeyspaceCreated(@Nonnull KeyspaceMetadata keyspace) {
    invokeListeners(listener -> listener.onKeyspaceCreated(keyspace), "onKeyspaceCreated");
  }

  @Override
  public void onKeyspaceDropped(@Nonnull KeyspaceMetadata keyspace) {
    invokeListeners(listener -> listener.onKeyspaceDropped(keyspace), "onKeyspaceDropped");
  }

  @Override
  public void onKeyspaceUpdated(
      @Nonnull KeyspaceMetadata current, @Nonnull KeyspaceMetadata previous) {
    invokeListeners(listener -> listener.onKeyspaceUpdated(current, previous), "onKeyspaceUpdated");
  }

  @Override
  public void onTableCreated(@Nonnull TableMetadata table) {
    invokeListeners(listener -> listener.onTableCreated(table), "onTableCreated");
  }

  @Override
  public void onTableDropped(@Nonnull TableMetadata table) {
    invokeListeners(listener -> listener.onTableDropped(table), "onTableDropped");
  }

  @Override
  public void onTableUpdated(@Nonnull TableMetadata current, @Nonnull TableMetadata previous) {
    invokeListeners(listener -> listener.onTableUpdated(current, previous), "onTableUpdated");
  }

  @Override
  public void onUserDefinedTypeCreated(@Nonnull UserDefinedType type) {
    invokeListeners(
        listener -> listener.onUserDefinedTypeCreated(type), "onUserDefinedTypeCreated");
  }

  @Override
  public void onUserDefinedTypeDropped(@Nonnull UserDefinedType type) {
    invokeListeners(
        listener -> listener.onUserDefinedTypeDropped(type), "onUserDefinedTypeDropped");
  }

  @Override
  public void onUserDefinedTypeUpdated(
      @Nonnull UserDefinedType current, @Nonnull UserDefinedType previous) {
    invokeListeners(
        listener -> listener.onUserDefinedTypeUpdated(current, previous),
        "onUserDefinedTypeUpdated");
  }

  @Override
  public void onFunctionCreated(@Nonnull FunctionMetadata function) {
    invokeListeners(listener -> listener.onFunctionCreated(function), "onFunctionCreated");
  }

  @Override
  public void onFunctionDropped(@Nonnull FunctionMetadata function) {
    invokeListeners(listener -> listener.onFunctionDropped(function), "onFunctionDropped");
  }

  @Override
  public void onFunctionUpdated(
      @Nonnull FunctionMetadata current, @Nonnull FunctionMetadata previous) {
    invokeListeners(listener -> listener.onFunctionUpdated(current, previous), "onFunctionUpdated");
  }

  @Override
  public void onAggregateCreated(@Nonnull AggregateMetadata aggregate) {
    invokeListeners(listener -> listener.onAggregateCreated(aggregate), "onAggregateCreated");
  }

  @Override
  public void onAggregateDropped(@Nonnull AggregateMetadata aggregate) {
    invokeListeners(listener -> listener.onAggregateDropped(aggregate), "onAggregateDropped");
  }

  @Override
  public void onAggregateUpdated(
      @Nonnull AggregateMetadata current, @Nonnull AggregateMetadata previous) {
    invokeListeners(
        listener -> listener.onAggregateUpdated(current, previous), "onAggregateUpdated");
  }

  @Override
  public void onViewCreated(@Nonnull ViewMetadata view) {
    invokeListeners(listener -> listener.onViewCreated(view), "onViewCreated");
  }

  @Override
  public void onViewDropped(@Nonnull ViewMetadata view) {
    invokeListeners(listener -> listener.onViewDropped(view), "onViewDropped");
  }

  @Override
  public void onViewUpdated(@Nonnull ViewMetadata current, @Nonnull ViewMetadata previous) {
    invokeListeners(listener -> listener.onViewUpdated(current, previous), "onViewUpdated");
  }

  @Override
  public void onSessionReady(@Nonnull Session session) {
    invokeListeners(listener -> listener.onSessionReady(session), "onSessionReady");
  }

  @Override
  public void close() throws Exception {
    for (SchemaChangeListener listener : listeners) {
      try {
        listener.close();
      } catch (Exception e) {
        Loggers.warnWithException(
            LOG, "Unexpected error while closing schema change listener {}.", listener, e);
      }
    }
  }

  private void invokeListeners(@Nonnull Consumer<SchemaChangeListener> action, String event) {
    for (SchemaChangeListener listener : listeners) {
      try {
        action.accept(listener);
      } catch (Exception e) {
        Loggers.warnWithException(
            LOG,
            "Unexpected error while notifying schema change listener {} of an {} event.",
            listener,
            event,
            e);
      }
    }
  }
}
