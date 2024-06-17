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
package com.datastax.oss.driver.internal.core.metadata.schema.events;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class KeyspaceChangeEvent {

  public static KeyspaceChangeEvent dropped(KeyspaceMetadata oldKeyspace) {
    return new KeyspaceChangeEvent(SchemaChangeType.DROPPED, oldKeyspace, null);
  }

  public static KeyspaceChangeEvent created(KeyspaceMetadata newKeyspace) {
    return new KeyspaceChangeEvent(SchemaChangeType.CREATED, null, newKeyspace);
  }

  public static KeyspaceChangeEvent updated(
      KeyspaceMetadata oldKeyspace, KeyspaceMetadata newKeyspace) {
    return new KeyspaceChangeEvent(SchemaChangeType.UPDATED, oldKeyspace, newKeyspace);
  }

  public final SchemaChangeType changeType;
  /** {@code null} if the event is a creation */
  public final KeyspaceMetadata oldKeyspace;
  /** {@code null} if the event is a drop */
  public final KeyspaceMetadata newKeyspace;

  private KeyspaceChangeEvent(
      SchemaChangeType changeType, KeyspaceMetadata oldKeyspace, KeyspaceMetadata newKeyspace) {
    this.changeType = changeType;
    this.oldKeyspace = oldKeyspace;
    this.newKeyspace = newKeyspace;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof KeyspaceChangeEvent) {
      KeyspaceChangeEvent that = (KeyspaceChangeEvent) other;
      return this.changeType == that.changeType
          && Objects.equals(this.oldKeyspace, that.oldKeyspace)
          && Objects.equals(this.newKeyspace, that.newKeyspace);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, oldKeyspace, newKeyspace);
  }

  @Override
  public String toString() {
    switch (changeType) {
      case CREATED:
        return String.format("KeyspaceChangeEvent(CREATED %s)", newKeyspace.getName());
      case UPDATED:
        return String.format(
            "KeyspaceChangeEvent(UPDATED %s=>%s)", oldKeyspace.getName(), newKeyspace.getName());
      case DROPPED:
        return String.format("KeyspaceChangeEvent(DROPPED %s)", oldKeyspace.getName());
      default:
        throw new IllegalStateException("Unsupported change type " + changeType);
    }
  }
}
