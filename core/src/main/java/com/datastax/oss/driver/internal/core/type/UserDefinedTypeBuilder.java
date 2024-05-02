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
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;

/**
 * Helper class to build {@link UserDefinedType} instances.
 *
 * <p>This is not part of the public API, because building user defined types manually can be
 * tricky: the fields must be defined in the exact same order as the database definition, otherwise
 * you will insert corrupt data in your database. If you decide to use this class anyway, make sure
 * that you define fields in the correct order, and that the database schema never changes.
 */
@NotThreadSafe
public class UserDefinedTypeBuilder {

  private final CqlIdentifier keyspaceName;
  private final CqlIdentifier typeName;
  private boolean frozen;
  private final ImmutableList.Builder<CqlIdentifier> fieldNames;
  private final ImmutableList.Builder<DataType> fieldTypes;
  private AttachmentPoint attachmentPoint = AttachmentPoint.NONE;

  public UserDefinedTypeBuilder(CqlIdentifier keyspaceName, CqlIdentifier typeName) {
    this.keyspaceName = keyspaceName;
    this.typeName = typeName;
    this.fieldNames = ImmutableList.builder();
    this.fieldTypes = ImmutableList.builder();
  }

  public UserDefinedTypeBuilder(String keyspaceName, String typeName) {
    this(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Adds a new field. The fields in the resulting type will be in the order of the calls to this
   * method.
   */
  public UserDefinedTypeBuilder withField(CqlIdentifier name, DataType type) {
    fieldNames.add(name);
    fieldTypes.add(type);
    return this;
  }

  public UserDefinedTypeBuilder withField(String name, DataType type) {
    return withField(CqlIdentifier.fromCql(name), type);
  }

  /** Makes the type frozen (by default, it is not). */
  public UserDefinedTypeBuilder frozen() {
    this.frozen = true;
    return this;
  }

  public UserDefinedTypeBuilder withAttachmentPoint(AttachmentPoint attachmentPoint) {
    this.attachmentPoint = attachmentPoint;
    return this;
  }

  public UserDefinedType build() {
    return new DefaultUserDefinedType(
        keyspaceName, typeName, frozen, fieldNames.build(), fieldTypes.build(), attachmentPoint);
  }
}
