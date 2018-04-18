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
package com.datastax.oss.driver.internal.core.metadata.schema.events;

import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class AggregateChangeEvent {

  public static AggregateChangeEvent dropped(AggregateMetadata oldAggregate) {
    return new AggregateChangeEvent(SchemaChangeType.DROPPED, oldAggregate, null);
  }

  public static AggregateChangeEvent created(AggregateMetadata newAggregate) {
    return new AggregateChangeEvent(SchemaChangeType.CREATED, null, newAggregate);
  }

  public static AggregateChangeEvent updated(
      AggregateMetadata oldAggregate, AggregateMetadata newAggregate) {
    return new AggregateChangeEvent(SchemaChangeType.UPDATED, oldAggregate, newAggregate);
  }

  public final SchemaChangeType changeType;
  /** {@code null} if the event is a creation */
  public final AggregateMetadata oldAggregate;
  /** {@code null} if the event is a drop */
  public final AggregateMetadata newAggregate;

  private AggregateChangeEvent(
      SchemaChangeType changeType, AggregateMetadata oldAggregate, AggregateMetadata newAggregate) {
    this.changeType = changeType;
    this.oldAggregate = oldAggregate;
    this.newAggregate = newAggregate;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof AggregateChangeEvent) {
      AggregateChangeEvent that = (AggregateChangeEvent) other;
      return this.changeType == that.changeType
          && Objects.equals(this.oldAggregate, that.oldAggregate)
          && Objects.equals(this.newAggregate, that.newAggregate);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, oldAggregate, newAggregate);
  }

  @Override
  public String toString() {
    switch (changeType) {
      case CREATED:
        return String.format("AggregateChangeEvent(CREATED %s)", newAggregate.getSignature());
      case UPDATED:
        return String.format(
            "AggregateChangeEvent(UPDATED %s=>%s)",
            oldAggregate.getSignature(), newAggregate.getSignature());
      case DROPPED:
        return String.format("AggregateChangeEvent(DROPPED %s)", oldAggregate.getSignature());
      default:
        throw new IllegalStateException("Unsupported change type " + changeType);
    }
  }
}
