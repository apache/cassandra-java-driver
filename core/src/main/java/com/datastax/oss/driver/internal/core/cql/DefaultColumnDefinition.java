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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DataTypeHelper;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultColumnDefinition implements ColumnDefinition, Serializable {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final CqlIdentifier keyspace;
  /** @serial */
  private final CqlIdentifier table;
  /** @serial */
  private final CqlIdentifier name;
  /** @serial */
  private final DataType type;

  /** @param spec the raw data decoded by the protocol layer */
  public DefaultColumnDefinition(
      @NonNull ColumnSpec spec, @NonNull AttachmentPoint attachmentPoint) {
    this.keyspace = CqlIdentifier.fromInternal(spec.ksName);
    this.table = CqlIdentifier.fromInternal(spec.tableName);
    this.name = CqlIdentifier.fromInternal(spec.name);
    this.type = DataTypeHelper.fromProtocolSpec(spec.type, attachmentPoint);
  }

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public CqlIdentifier getTable() {
    return table;
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @NonNull
  @Override
  public DataType getType() {
    return type;
  }

  @Override
  public boolean isDetached() {
    return type.isDetached();
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    type.attach(attachmentPoint);
  }

  @Override
  public String toString() {
    return keyspace.asCql(true) + "." + table.asCql(true) + "." + name.asCql(true) + " " + type;
  }
}
