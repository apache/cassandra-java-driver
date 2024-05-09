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

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.util.Locale;
import net.jcip.annotations.Immutable;

@Immutable
public class PrimitiveType implements DataType, Serializable {

  /** @serial */
  private final int protocolCode;

  public PrimitiveType(int protocolCode) {
    this.protocolCode = protocolCode;
  }

  @Override
  public int getProtocolCode() {
    return protocolCode;
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    // nothing to do
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof PrimitiveType) {
      PrimitiveType that = (PrimitiveType) other;
      return this.protocolCode == that.protocolCode;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return protocolCode;
  }

  @NonNull
  @Override
  public String asCql(boolean includeFrozen, boolean pretty) {
    return codeName(protocolCode).toLowerCase(Locale.ROOT);
  }

  @Override
  public String toString() {
    return codeName(protocolCode);
  }

  private static String codeName(int protocolCode) {
    // Reminder: we don't use enums to leave the door open for custom extensions
    switch (protocolCode) {
      case ProtocolConstants.DataType.ASCII:
        return "ASCII";
      case ProtocolConstants.DataType.BIGINT:
        return "BIGINT";
      case ProtocolConstants.DataType.BLOB:
        return "BLOB";
      case ProtocolConstants.DataType.BOOLEAN:
        return "BOOLEAN";
      case ProtocolConstants.DataType.COUNTER:
        return "COUNTER";
      case ProtocolConstants.DataType.DECIMAL:
        return "DECIMAL";
      case ProtocolConstants.DataType.DOUBLE:
        return "DOUBLE";
      case ProtocolConstants.DataType.FLOAT:
        return "FLOAT";
      case ProtocolConstants.DataType.INT:
        return "INT";
      case ProtocolConstants.DataType.TIMESTAMP:
        return "TIMESTAMP";
      case ProtocolConstants.DataType.UUID:
        return "UUID";
      case ProtocolConstants.DataType.VARINT:
        return "VARINT";
      case ProtocolConstants.DataType.TIMEUUID:
        return "TIMEUUID";
      case ProtocolConstants.DataType.INET:
        return "INET";
      case ProtocolConstants.DataType.DATE:
        return "DATE";
      case ProtocolConstants.DataType.VARCHAR:
        return "TEXT";
      case ProtocolConstants.DataType.TIME:
        return "TIME";
      case ProtocolConstants.DataType.SMALLINT:
        return "SMALLINT";
      case ProtocolConstants.DataType.TINYINT:
        return "TINYINT";
      case ProtocolConstants.DataType.DURATION:
        return "DURATION";
      default:
        return "0x" + Integer.toHexString(protocolCode);
    }
  }
}
