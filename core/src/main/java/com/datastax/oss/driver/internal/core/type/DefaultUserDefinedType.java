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
package com.datastax.oss.driver.internal.core.type;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.driver.internal.core.data.IdentifierIndex;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultUserDefinedType implements UserDefinedType, Serializable {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final CqlIdentifier keyspace;
  /** @serial */
  private final CqlIdentifier name;

  // Data types are only [de]serialized as part of a row, frozenness doesn't matter in that context
  private final transient boolean frozen;

  /** @serial */
  private final List<CqlIdentifier> fieldNames;
  /** @serial */
  private final List<DataType> fieldTypes;

  private transient IdentifierIndex index;
  private transient volatile AttachmentPoint attachmentPoint;

  public DefaultUserDefinedType(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier name,
      boolean frozen,
      List<CqlIdentifier> fieldNames,
      @NonNull List<DataType> fieldTypes,
      @NonNull AttachmentPoint attachmentPoint) {
    Preconditions.checkNotNull(keyspace);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldNames);
    Preconditions.checkNotNull(fieldTypes);
    Preconditions.checkArgument(fieldNames.size() > 0, "Field names list can't be null or empty");
    Preconditions.checkArgument(
        fieldTypes.size() == fieldNames.size(),
        "There should be the same number of field names and types");
    this.keyspace = keyspace;
    this.name = name;
    this.frozen = frozen;
    this.fieldNames = ImmutableList.copyOf(fieldNames);
    this.fieldTypes = ImmutableList.copyOf(fieldTypes);
    this.index = new IdentifierIndex(this.fieldNames);
    this.attachmentPoint = attachmentPoint;
  }

  public DefaultUserDefinedType(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier name,
      boolean frozen,
      @NonNull List<CqlIdentifier> fieldNames,
      @NonNull List<DataType> fieldTypes) {
    this(keyspace, name, frozen, fieldNames, fieldTypes, AttachmentPoint.NONE);
  }

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @Override
  public boolean isFrozen() {
    return frozen;
  }

  @NonNull
  @Override
  public List<CqlIdentifier> getFieldNames() {
    return fieldNames;
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    return index.allIndicesOf(id);
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    return index.firstIndexOf(id);
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull String name) {
    return index.allIndicesOf(name);
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    return index.firstIndexOf(name);
  }

  @NonNull
  @Override
  public List<DataType> getFieldTypes() {
    return fieldTypes;
  }

  @NonNull
  @Override
  public UserDefinedType copy(boolean newFrozen) {
    return (newFrozen == frozen)
        ? this
        : new DefaultUserDefinedType(
            keyspace, name, newFrozen, fieldNames, fieldTypes, attachmentPoint);
  }

  @NonNull
  @Override
  public UdtValue newValue() {
    return new DefaultUdtValue(this);
  }

  @NonNull
  @Override
  public UdtValue newValue(@NonNull Object... fields) {
    return new DefaultUdtValue(this, fields);
  }

  @Override
  public boolean isDetached() {
    return attachmentPoint == AttachmentPoint.NONE;
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    this.attachmentPoint = attachmentPoint;
    for (DataType fieldType : fieldTypes) {
      fieldType.attach(attachmentPoint);
    }
  }

  @NonNull
  @Override
  public AttachmentPoint getAttachmentPoint() {
    return attachmentPoint;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof UserDefinedType) {
      UserDefinedType that = (UserDefinedType) other;
      // frozen is ignored in comparisons
      return this.keyspace.equals(that.getKeyspace())
          && this.name.equals(that.getName())
          && this.fieldNames.equals(that.getFieldNames())
          && this.fieldTypes.equals(that.getFieldTypes());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspace, name, fieldNames, fieldTypes);
  }

  @Override
  public String toString() {
    return "UDT(" + keyspace.asCql(true) + "." + name.asCql(true) + ")";
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Preconditions.checkNotNull(keyspace);
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(
        fieldNames != null && fieldNames.size() > 0, "Field names list can't be null or empty");
    Preconditions.checkArgument(
        fieldTypes != null && fieldTypes.size() == fieldNames.size(),
        "There should be the same number of field names and types");
    this.attachmentPoint = AttachmentPoint.NONE;
    this.index = new IdentifierIndex(this.fieldNames);
  }
}
