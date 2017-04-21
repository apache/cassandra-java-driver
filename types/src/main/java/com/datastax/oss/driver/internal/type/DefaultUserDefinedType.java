/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.type;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.UserDefinedType;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;

public class DefaultUserDefinedType implements UserDefinedType {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier name;
  private final Map<CqlIdentifier, DataType> fieldTypes;

  public DefaultUserDefinedType(
      CqlIdentifier keyspace, CqlIdentifier name, Map<CqlIdentifier, DataType> fieldTypes) {
    Preconditions.checkNotNull(keyspace);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldTypes);
    this.keyspace = keyspace;
    this.name = name;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @Override
  public Map<CqlIdentifier, DataType> getFieldTypes() {
    return fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof UserDefinedType) {
      UserDefinedType that = (UserDefinedType) other;
      return this.keyspace.equals(that.getKeyspace())
          && this.name.equals(that.getName())
          && this.fieldTypes.equals(that.getFieldTypes());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspace, name, fieldTypes);
  }

  @Override
  public String toString() {
    return "UDT(" + keyspace.asPrettyCql() + "." + name.asPrettyCql() + ")";
  }
}
