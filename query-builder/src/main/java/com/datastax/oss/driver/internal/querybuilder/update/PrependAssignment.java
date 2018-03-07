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
package com.datastax.oss.driver.internal.querybuilder.update;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

public class PrependAssignment implements Assignment {

  private final CqlIdentifier columnId;
  private final Term prefix;

  public PrependAssignment(CqlIdentifier columnId, Term prefix) {
    this.columnId = columnId;
    this.prefix = prefix;
  }

  @Override
  public void appendTo(StringBuilder builder) {
    String column = columnId.asCql(true);
    builder.append(column).append('=');
    prefix.appendTo(builder);
    builder.append('+').append(column);
  }

  @Override
  public boolean isIdempotent() {
    // Not idempotent for lists, be pessimistic
    return false;
  }

  public CqlIdentifier getColumnId() {
    return columnId;
  }

  public Term getPrefix() {
    return prefix;
  }
}
