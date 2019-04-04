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
package com.datastax.oss.driver.internal.querybuilder.truncate;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;

public class DefaultTruncate implements Truncate {
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;

  public DefaultTruncate(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    this.keyspace = keyspace;
    this.table = table;
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();
    builder.append("TRUNCATE ");
    CqlHelper.qualify(keyspace, table, builder);
    return builder.toString();
  }

  @NonNull
  @Override
  public SimpleStatementBuilder builder() {
    return SimpleStatement.builder(asCql()).setIdempotence(true);
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Object... values) {
    throw new UnsupportedOperationException(
        "TRUNCATE doesn't take values as parameters. Use build() method instead.");
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Map<String, Object> namedValues) {
    throw new UnsupportedOperationException(
        "TRUNCATE doesn't take namedValues as parameters. Use build() method instead.");
  }

  @NonNull
  @Override
  public SimpleStatement build() {
    return builder().build();
  }
}
