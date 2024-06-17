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
package com.datastax.oss.driver.internal.core.metadata.schema.refresh;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.events.AggregateChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.FunctionChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.ViewChangeEvent;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class SchemaRefresh implements MetadataRefresh {

  @VisibleForTesting public final Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces;

  public SchemaRefresh(Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces) {
    this.newKeyspaces = newKeyspaces;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {
    ImmutableList.Builder<Object> events = ImmutableList.builder();

    Map<CqlIdentifier, KeyspaceMetadata> oldKeyspaces = oldMetadata.getKeyspaces();
    for (CqlIdentifier removedKey : Sets.difference(oldKeyspaces.keySet(), newKeyspaces.keySet())) {
      events.add(KeyspaceChangeEvent.dropped(oldKeyspaces.get(removedKey)));
    }
    for (Map.Entry<CqlIdentifier, KeyspaceMetadata> entry : newKeyspaces.entrySet()) {
      CqlIdentifier key = entry.getKey();
      computeEvents(oldKeyspaces.get(key), entry.getValue(), events);
    }

    return new Result(
        oldMetadata.withSchema(this.newKeyspaces, tokenMapEnabled, context), events.build());
  }

  /**
   * Computes the exact set of events to emit when a keyspace has changed.
   *
   * <p>We can't simply emit {@link KeyspaceChangeEvent#updated(KeyspaceMetadata, KeyspaceMetadata)}
   * because this method might be called as part of a full schema refresh, or a keyspace refresh
   * initiated by coalesced child element refreshes. We need to traverse all children to check what
   * has exactly changed.
   */
  private void computeEvents(
      KeyspaceMetadata oldKeyspace,
      KeyspaceMetadata newKeyspace,
      ImmutableList.Builder<Object> events) {
    if (oldKeyspace == null) {
      events.add(KeyspaceChangeEvent.created(newKeyspace));
    } else {
      if (!oldKeyspace.shallowEquals(newKeyspace)) {
        events.add(KeyspaceChangeEvent.updated(oldKeyspace, newKeyspace));
      }
      computeChildEvents(oldKeyspace, newKeyspace, events);
    }
  }

  private void computeChildEvents(
      KeyspaceMetadata oldKeyspace,
      KeyspaceMetadata newKeyspace,
      ImmutableList.Builder<Object> events) {
    computeChildEvents(
        oldKeyspace.getTables(),
        newKeyspace.getTables(),
        TableChangeEvent::dropped,
        TableChangeEvent::created,
        TableChangeEvent::updated,
        events);
    computeChildEvents(
        oldKeyspace.getViews(),
        newKeyspace.getViews(),
        ViewChangeEvent::dropped,
        ViewChangeEvent::created,
        ViewChangeEvent::updated,
        events);
    computeChildEvents(
        oldKeyspace.getUserDefinedTypes(),
        newKeyspace.getUserDefinedTypes(),
        TypeChangeEvent::dropped,
        TypeChangeEvent::created,
        TypeChangeEvent::updated,
        events);
    computeChildEvents(
        oldKeyspace.getFunctions(),
        newKeyspace.getFunctions(),
        FunctionChangeEvent::dropped,
        FunctionChangeEvent::created,
        FunctionChangeEvent::updated,
        events);
    computeChildEvents(
        oldKeyspace.getAggregates(),
        newKeyspace.getAggregates(),
        AggregateChangeEvent::dropped,
        AggregateChangeEvent::created,
        AggregateChangeEvent::updated,
        events);
  }

  private <K, V> void computeChildEvents(
      Map<K, V> oldChildren,
      Map<K, V> newChildren,
      Function<V, Object> newDroppedEvent,
      Function<V, Object> newCreatedEvent,
      BiFunction<V, V, Object> newUpdatedEvent,
      ImmutableList.Builder<Object> events) {
    for (K removedKey : Sets.difference(oldChildren.keySet(), newChildren.keySet())) {
      events.add(newDroppedEvent.apply(oldChildren.get(removedKey)));
    }
    for (Map.Entry<K, V> entry : newChildren.entrySet()) {
      K key = entry.getKey();
      V newChild = entry.getValue();
      V oldChild = oldChildren.get(key);
      if (oldChild == null) {
        events.add(newCreatedEvent.apply(newChild));
      } else if (!oldChild.equals(newChild)) {
        events.add(newUpdatedEvent.apply(oldChild, newChild));
      }
    }
  }
}
