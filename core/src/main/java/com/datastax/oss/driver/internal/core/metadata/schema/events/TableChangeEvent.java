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

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class TableChangeEvent {

  public static TableChangeEvent dropped(TableMetadata oldTable) {
    return new TableChangeEvent(SchemaChangeType.DROPPED, oldTable, null);
  }

  public static TableChangeEvent created(TableMetadata newTable) {
    return new TableChangeEvent(SchemaChangeType.CREATED, null, newTable);
  }

  public static TableChangeEvent updated(TableMetadata oldTable, TableMetadata newTable) {
    return new TableChangeEvent(SchemaChangeType.UPDATED, oldTable, newTable);
  }

  public final SchemaChangeType changeType;
  /** {@code null} if the event is a creation */
  public final TableMetadata oldTable;
  /** {@code null} if the event is a drop */
  public final TableMetadata newTable;

  private TableChangeEvent(
      SchemaChangeType changeType, TableMetadata oldTable, TableMetadata newTable) {
    this.changeType = changeType;
    this.oldTable = oldTable;
    this.newTable = newTable;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TableChangeEvent) {
      TableChangeEvent that = (TableChangeEvent) other;
      return this.changeType == that.changeType
          && Objects.equals(this.oldTable, that.oldTable)
          && Objects.equals(this.newTable, that.newTable);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, oldTable, newTable);
  }

  @Override
  public String toString() {
    switch (changeType) {
      case CREATED:
        return String.format("TableChangeEvent(CREATED %s)", newTable.getName());
      case UPDATED:
        return String.format(
            "TableChangeEvent(UPDATED %s=>%s)", oldTable.getName(), newTable.getName());
      case DROPPED:
        return String.format("TableChangeEvent(DROPPED %s)", oldTable.getName());
      default:
        throw new IllegalStateException("Unsupported change type " + changeType);
    }
  }
}
