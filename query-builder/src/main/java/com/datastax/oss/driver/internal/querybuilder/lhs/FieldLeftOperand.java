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
package com.datastax.oss.driver.internal.querybuilder.lhs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class FieldLeftOperand implements LeftOperand {

  private final CqlIdentifier columnId;
  private final CqlIdentifier fieldId;

  public FieldLeftOperand(@NonNull CqlIdentifier columnId, @NonNull CqlIdentifier fieldId) {
    this.columnId = columnId;
    this.fieldId = fieldId;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(columnId.asCql(true)).append('.').append(fieldId.asCql(true));
  }

  @NonNull
  public CqlIdentifier getColumnId() {
    return columnId;
  }

  @NonNull
  public CqlIdentifier getFieldId() {
    return fieldId;
  }
}
