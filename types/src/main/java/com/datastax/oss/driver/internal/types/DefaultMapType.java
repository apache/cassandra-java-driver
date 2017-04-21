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
package com.datastax.oss.driver.internal.types;

import com.datastax.oss.driver.api.types.DataType;
import com.datastax.oss.driver.api.types.MapType;
import com.google.common.base.Preconditions;
import java.util.Objects;

public class DefaultMapType implements MapType {
  private final DataType keyType;
  private final DataType valueType;
  private final boolean frozen;

  public DefaultMapType(DataType keyType, DataType valueType, boolean frozen) {
    Preconditions.checkNotNull(keyType);
    Preconditions.checkNotNull(valueType);
    this.keyType = keyType;
    this.valueType = valueType;
    this.frozen = frozen;
  }

  @Override
  public DataType getKeyType() {
    return keyType;
  }

  @Override
  public DataType getValueType() {
    return valueType;
  }

  @Override
  public boolean isFrozen() {
    return frozen;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof MapType) {
      MapType that = (MapType) other;
      // frozen is not taken into account
      return this.keyType.equals(that.getKeyType()) && this.valueType.equals(that.getValueType());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType);
  }

  @Override
  public String toString() {
    return "Map(" + keyType + " => " + valueType + ", " + (frozen ? "" : "not ") + "frozen)";
  }
}
