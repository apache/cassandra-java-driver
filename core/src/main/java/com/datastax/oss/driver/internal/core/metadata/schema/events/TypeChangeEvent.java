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

import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class TypeChangeEvent {

  public static TypeChangeEvent dropped(UserDefinedType oldType) {
    return new TypeChangeEvent(SchemaChangeType.DROPPED, oldType, null);
  }

  public static TypeChangeEvent created(UserDefinedType newType) {
    return new TypeChangeEvent(SchemaChangeType.CREATED, null, newType);
  }

  public static TypeChangeEvent updated(UserDefinedType oldType, UserDefinedType newType) {
    return new TypeChangeEvent(SchemaChangeType.UPDATED, oldType, newType);
  }

  public final SchemaChangeType changeType;
  /** {@code null} if the event is a creation */
  public final UserDefinedType oldType;
  /** {@code null} if the event is a drop */
  public final UserDefinedType newType;

  private TypeChangeEvent(
      SchemaChangeType changeType, UserDefinedType oldType, UserDefinedType newType) {
    this.changeType = changeType;
    this.oldType = oldType;
    this.newType = newType;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TypeChangeEvent) {
      TypeChangeEvent that = (TypeChangeEvent) other;
      return this.changeType == that.changeType
          && Objects.equals(this.oldType, that.oldType)
          && Objects.equals(this.newType, that.newType);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, oldType, newType);
  }

  @Override
  public String toString() {
    switch (changeType) {
      case CREATED:
        return String.format("TypeChangeEvent(CREATED %s)", newType.getName());
      case UPDATED:
        return String.format(
            "TypeChangeEvent(UPDATED %s=>%s)", oldType.getName(), newType.getName());
      case DROPPED:
        return String.format("TypeChangeEvent(DROPPED %s)", oldType.getName());
      default:
        throw new IllegalStateException("Unsupported change type " + changeType);
    }
  }
}
