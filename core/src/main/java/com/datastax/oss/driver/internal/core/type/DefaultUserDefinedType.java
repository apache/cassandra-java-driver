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
package com.datastax.oss.driver.internal.core.type;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.driver.internal.core.data.IdentifierIndex;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Objects;

public class DefaultUserDefinedType implements UserDefinedType {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final CqlIdentifier keyspace;
  /** @serial */
  private final CqlIdentifier name;
  /** @serial */
  private final List<CqlIdentifier> fieldNames;
  /** @serial */
  private final List<DataType> fieldTypes;

  private transient IdentifierIndex index;
  private transient volatile AttachmentPoint attachmentPoint;

  public DefaultUserDefinedType(
      CqlIdentifier keyspace,
      CqlIdentifier name,
      List<CqlIdentifier> fieldNames,
      List<DataType> fieldTypes,
      AttachmentPoint attachmentPoint) {
    Preconditions.checkNotNull(keyspace);
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(
        fieldNames != null && fieldNames.size() > 0, "Field names list can't be null or empty");
    Preconditions.checkArgument(
        fieldTypes != null && fieldTypes.size() == fieldNames.size(),
        "There should be the same number of field names and types");
    this.keyspace = keyspace;
    this.name = name;
    this.fieldNames = ImmutableList.copyOf(fieldNames);
    this.fieldTypes = ImmutableList.copyOf(fieldTypes);
    this.index = new IdentifierIndex(this.fieldNames);
    this.attachmentPoint = attachmentPoint;
  }

  public DefaultUserDefinedType(
      CqlIdentifier keyspace,
      CqlIdentifier name,
      List<CqlIdentifier> fieldNames,
      List<DataType> fieldTypes) {
    this(keyspace, name, fieldNames, fieldTypes, AttachmentPoint.NONE);
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
  public List<CqlIdentifier> getFieldNames() {
    return fieldNames;
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return index.firstIndexOf(id);
  }

  @Override
  public int firstIndexOf(String name) {
    return index.firstIndexOf(name);
  }

  @Override
  public List<DataType> getFieldTypes() {
    return fieldTypes;
  }

  @Override
  public UdtValue newValue() {
    return new DefaultUdtValue(this);
  }

  @Override
  public boolean isDetached() {
    return attachmentPoint == AttachmentPoint.NONE;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    this.attachmentPoint = attachmentPoint;
    for (DataType fieldType : fieldTypes) {
      fieldType.attach(attachmentPoint);
    }
  }

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
    return "UDT(" + keyspace.asPrettyCql() + "." + name.asPrettyCql() + ")";
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
