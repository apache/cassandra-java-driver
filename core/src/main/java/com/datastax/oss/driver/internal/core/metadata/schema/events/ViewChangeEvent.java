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

import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class ViewChangeEvent {

  public static ViewChangeEvent dropped(ViewMetadata oldView) {
    return new ViewChangeEvent(SchemaChangeType.DROPPED, oldView, null);
  }

  public static ViewChangeEvent created(ViewMetadata newView) {
    return new ViewChangeEvent(SchemaChangeType.CREATED, null, newView);
  }

  public static ViewChangeEvent updated(ViewMetadata oldView, ViewMetadata newView) {
    return new ViewChangeEvent(SchemaChangeType.UPDATED, oldView, newView);
  }

  public final SchemaChangeType changeType;
  /** {@code null} if the event is a creation */
  public final ViewMetadata oldView;
  /** {@code null} if the event is a drop */
  public final ViewMetadata newView;

  private ViewChangeEvent(SchemaChangeType changeType, ViewMetadata oldView, ViewMetadata newView) {
    this.changeType = changeType;
    this.oldView = oldView;
    this.newView = newView;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ViewChangeEvent) {
      ViewChangeEvent that = (ViewChangeEvent) other;
      return this.changeType == that.changeType
          && Objects.equals(this.oldView, that.oldView)
          && Objects.equals(this.newView, that.newView);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, oldView, newView);
  }

  @Override
  public String toString() {
    switch (changeType) {
      case CREATED:
        return String.format("ViewChangeEvent(CREATED %s)", newView.getName());
      case UPDATED:
        return String.format(
            "ViewChangeEvent(UPDATED %s=>%s)", oldView.getName(), newView.getName());
      case DROPPED:
        return String.format("ViewChangeEvent(DROPPED %s)", oldView.getName());
      default:
        throw new IllegalStateException("Unsupported change type " + changeType);
    }
  }
}
