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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDrop implements Drop {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier itemName;
  private final String schemaTypeName;

  private final boolean ifExists;

  public DefaultDrop(CqlIdentifier itemName, String schemaTypeName) {
    this(null, itemName, schemaTypeName);
  }

  public DefaultDrop(CqlIdentifier keyspace, CqlIdentifier itemName, String schemaTypeName) {
    this(keyspace, itemName, schemaTypeName, false);
  }

  public DefaultDrop(
      CqlIdentifier keyspace, CqlIdentifier itemName, String schemaTypeName, boolean ifExists) {
    this.keyspace = keyspace;
    this.itemName = itemName;
    this.schemaTypeName = schemaTypeName;
    this.ifExists = ifExists;
  }

  @Override
  public Drop ifExists() {
    return new DefaultDrop(keyspace, itemName, schemaTypeName, true);
  }

  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("DROP ").append(schemaTypeName).append(' ');

    if (ifExists) {
      builder.append("IF EXISTS ");
    }

    CqlHelper.qualify(keyspace, itemName, builder);

    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  public CqlIdentifier getName() {
    return itemName;
  }

  public String getSchemaType() {
    return schemaTypeName;
  }

  public boolean isIfExists() {
    return ifExists;
  }
}
