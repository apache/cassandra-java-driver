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
package com.datastax.oss.driver.internal.querybuilder.truncate;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DefaultTruncate implements Truncate {
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;

  public DefaultTruncate(@Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    this.keyspace = keyspace;
    this.table = table;
  }

  @Nonnull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();
    builder.append("TRUNCATE ");
    CqlHelper.qualify(keyspace, table, builder);
    return builder.toString();
  }

  @Nonnull
  @Override
  public SimpleStatementBuilder builder() {
    return SimpleStatement.builder(asCql()).setIdempotence(true);
  }

  @Nonnull
  @Override
  public SimpleStatement build(@Nonnull Object... values) {
    throw new UnsupportedOperationException(
        "TRUNCATE doesn't take values as parameters. Use build() method instead.");
  }

  @Nonnull
  @Override
  public SimpleStatement build(@Nonnull Map<String, Object> namedValues) {
    throw new UnsupportedOperationException(
        "TRUNCATE doesn't take namedValues as parameters. Use build() method instead.");
  }

  @Nonnull
  @Override
  public SimpleStatement build() {
    return builder().build();
  }
}
