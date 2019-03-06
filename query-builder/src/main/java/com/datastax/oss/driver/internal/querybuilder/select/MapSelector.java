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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class MapSelector implements Selector {

  private final Map<Selector, Selector> elementSelectors;
  private final DataType keyType;
  private final DataType valueType;
  private final CqlIdentifier alias;

  public MapSelector(
      @NonNull Map<Selector, Selector> elementSelectors,
      @Nullable DataType keyType,
      @Nullable DataType valueType) {
    this(elementSelectors, keyType, valueType, null);
  }

  public MapSelector(
      @NonNull Map<Selector, Selector> elementSelectors,
      @Nullable DataType keyType,
      @Nullable DataType valueType,
      @Nullable CqlIdentifier alias) {
    Preconditions.checkNotNull(elementSelectors);
    Preconditions.checkArgument(
        !elementSelectors.isEmpty(), "Must have at least one key/value pair");
    checkNoAlias(elementSelectors);
    Preconditions.checkArgument(
        (keyType == null) == (valueType == null),
        "Key and value type must be either both null or both non-null");
    this.elementSelectors = elementSelectors;
    this.keyType = keyType;
    this.valueType = valueType;
    this.alias = alias;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new MapSelector(elementSelectors, keyType, valueType, alias);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    if (keyType != null) {
      assert valueType != null;
      builder
          .append("(map<")
          .append(keyType.asCql(false, true))
          .append(',')
          .append(valueType.asCql(false, true))
          .append(">)");
    }
    builder.append("{");
    boolean first = true;
    for (Map.Entry<Selector, Selector> entry : elementSelectors.entrySet()) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      entry.getKey().appendTo(builder);
      builder.append(":");
      entry.getValue().appendTo(builder);
    }
    builder.append("}");

    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @NonNull
  public Map<Selector, Selector> getElementSelectors() {
    return elementSelectors;
  }

  @Nullable
  public DataType getKeyType() {
    return keyType;
  }

  @Nullable
  public DataType getValueType() {
    return valueType;
  }

  @Nullable
  @Override
  public CqlIdentifier getAlias() {
    return alias;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof MapSelector) {
      MapSelector that = (MapSelector) other;
      return this.elementSelectors.equals(that.elementSelectors)
          && Objects.equals(this.keyType, that.keyType)
          && Objects.equals(this.valueType, that.valueType)
          && Objects.equals(this.alias, that.alias);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementSelectors, keyType, valueType, alias);
  }

  private static void checkNoAlias(Map<Selector, Selector> elementSelectors) {
    String offendingAliases = null;
    for (Map.Entry<Selector, Selector> entry : elementSelectors.entrySet()) {
      offendingAliases = appendIfNotNull(offendingAliases, entry.getKey().getAlias());
      offendingAliases = appendIfNotNull(offendingAliases, entry.getValue().getAlias());
    }
    if (offendingAliases != null) {
      throw new IllegalArgumentException(
          "Can't use aliases in selection map, offending aliases: " + offendingAliases);
    }
  }

  private static String appendIfNotNull(String offendingAliases, CqlIdentifier alias) {
    if (alias == null) {
      return offendingAliases;
    } else if (offendingAliases == null) {
      return alias.asCql(true);
    } else {
      return offendingAliases + ", " + alias.asCql(true);
    }
  }
}
