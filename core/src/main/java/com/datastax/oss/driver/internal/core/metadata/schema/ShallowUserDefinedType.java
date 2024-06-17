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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.UserDefinedTypeParser;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import net.jcip.annotations.Immutable;

/**
 * A temporary UDT implementation that only contains the keyspace and name.
 *
 * <p>When we refresh a keyspace's UDTs, we can't fully materialize them right away, because they
 * might depend on each other and the system table query does not return them in topological order.
 * So we do a first pass where UDTs that are nested into other UDTsare resolved as instances of this
 * class, then a topological sort, then a second pass to replace all shallow definitions by the
 * actual instance (which will be a {@link DefaultUserDefinedType}).
 *
 * <p>This type is also used in the schema builder's internal representation: the keyspace, name and
 * frozen-ness are the only things we need to generate a query string.
 *
 * @see UserDefinedTypeParser
 */
@Immutable
public class ShallowUserDefinedType implements UserDefinedType, Serializable {

  private static final long serialVersionUID = 1;

  private final CqlIdentifier keyspace;
  private final CqlIdentifier name;
  private final boolean frozen;

  public ShallowUserDefinedType(CqlIdentifier keyspace, CqlIdentifier name, boolean frozen) {
    this.keyspace = keyspace;
    this.name = name;
    this.frozen = frozen;
  }

  @Nullable
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
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull String name) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public List<DataType> getFieldTypes() {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public UserDefinedType copy(boolean newFrozen) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public UdtValue newValue() {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public UdtValue newValue(@NonNull Object... fields) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @NonNull
  @Override
  public AttachmentPoint getAttachmentPoint() {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @Override
  public boolean isDetached() {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  private void readObject(@SuppressWarnings("unused") ObjectInputStream s) throws IOException {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }

  private void writeObject(@SuppressWarnings("unused") ObjectOutputStream s) throws IOException {
    throw new UnsupportedOperationException(
        "This implementation should only be used internally, this is likely a driver bug");
  }
}
