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

import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class FunctionChangeEvent {

  public static FunctionChangeEvent dropped(FunctionMetadata oldFunction) {
    return new FunctionChangeEvent(SchemaChangeType.DROPPED, oldFunction, null);
  }

  public static FunctionChangeEvent created(FunctionMetadata newFunction) {
    return new FunctionChangeEvent(SchemaChangeType.CREATED, null, newFunction);
  }

  public static FunctionChangeEvent updated(
      FunctionMetadata oldFunction, FunctionMetadata newFunction) {
    return new FunctionChangeEvent(SchemaChangeType.UPDATED, oldFunction, newFunction);
  }

  public final SchemaChangeType changeType;
  /** {@code null} if the event is a creation */
  public final FunctionMetadata oldFunction;
  /** {@code null} if the event is a drop */
  public final FunctionMetadata newFunction;

  private FunctionChangeEvent(
      SchemaChangeType changeType, FunctionMetadata oldFunction, FunctionMetadata newFunction) {
    this.changeType = changeType;
    this.oldFunction = oldFunction;
    this.newFunction = newFunction;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof FunctionChangeEvent) {
      FunctionChangeEvent that = (FunctionChangeEvent) other;
      return this.changeType == that.changeType
          && Objects.equals(this.oldFunction, that.oldFunction)
          && Objects.equals(this.newFunction, that.newFunction);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, oldFunction, newFunction);
  }

  @Override
  public String toString() {
    switch (changeType) {
      case CREATED:
        return String.format("FunctionChangeEvent(CREATED %s)", newFunction.getSignature());
      case UPDATED:
        return String.format(
            "FunctionChangeEvent(UPDATED %s=>%s)",
            oldFunction.getSignature(), newFunction.getSignature());
      case DROPPED:
        return String.format("FunctionChangeEvent(DROPPED %s)", oldFunction.getSignature());
      default:
        throw new IllegalStateException("Unsupported change type " + changeType);
    }
  }
}
