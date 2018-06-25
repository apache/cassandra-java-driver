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
package com.datastax.oss.driver.internal.querybuilder.lhs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class ColumnLeftOperand implements LeftOperand {

  private final CqlIdentifier columnId;

  public ColumnLeftOperand(@NonNull CqlIdentifier columnId) {
    this.columnId = columnId;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(columnId.asCql(true));
  }

  @NonNull
  public CqlIdentifier getColumnId() {
    return columnId;
  }
}
