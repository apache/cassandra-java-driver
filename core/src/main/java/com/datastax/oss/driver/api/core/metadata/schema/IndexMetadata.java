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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.google.common.collect.Maps;
import java.util.Map;

/** A secondary index in the schema metadata. */
public interface IndexMetadata extends Describable {

  CqlIdentifier getKeyspace();

  CqlIdentifier getTable();

  CqlIdentifier getName();

  IndexKind getKind();

  String getTarget();

  /**
   * If this index is custom, the name of the server-side implementation. Otherwise, {@code null}.
   */
  default String getClassName() {
    return getOptions().get("class_name");
  }

  /**
   * The options of the index.
   *
   * <p>This directly reflects the corresponding column of the system table ( {@code
   * system.schema_columns.index_options} in Cassandra <= 2.2, or {@code
   * system_schema.indexes.options} in later versions).
   *
   * <p>Note that some of these options might also be exposed as standalone fields in this
   * interface, namely {@link #getClassName()} and {{@link #getTarget()}}.
   */
  Map<String, String> getOptions();

  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    if (getClassName() != null) {
      builder
          .append("CREATE CUSTOM INDEX ")
          .append(getName())
          .append(" ON ")
          .append(getKeyspace())
          .append(".")
          .append(getTable())
          .append(String.format(" (%s)", getTarget()))
          .newLine()
          .append(String.format("USING '%s'", getClassName()));

      // Some options already appear in the CREATE statement, ignore them
      Map<String, String> describedOptions =
          Maps.filterKeys(getOptions(), k -> !"target".equals(k) && !"class_name".equals(k));
      if (!describedOptions.isEmpty()) {
        builder.newLine().append("WITH OPTIONS = {").newLine().increaseIndent();
        boolean first = true;
        for (Map.Entry<String, String> option : describedOptions.entrySet()) {
          if (first) {
            first = false;
          } else {
            builder.append(",").newLine();
          }
          builder.append(String.format("'%s' : '%s'", option.getKey(), option.getValue()));
        }
        builder.decreaseIndent().append("}");
      }
    } else {
      builder
          .append("CREATE INDEX ")
          .append(getName())
          .append(" ON ")
          .append(getKeyspace())
          .append(".")
          .append(getTable())
          .append(String.format(" (%s);", getTarget()));
    }
    return builder.build();
  }

  @Override
  default String describeWithChildren(boolean pretty) {
    // An index has no children
    return describe(pretty);
  }
}
