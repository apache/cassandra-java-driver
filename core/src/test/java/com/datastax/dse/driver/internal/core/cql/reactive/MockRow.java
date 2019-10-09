/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

class MockRow implements Row {

  private int index;

  MockRow(int index) {
    this.index = index;
  }

  @Override
  public int size() {
    return 0;
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    return mock(CodecRegistry.class);
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    return DefaultProtocolVersion.V4;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return EmptyColumnDefinitions.INSTANCE;
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    return 0;
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    return 0;
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return DataTypes.INT;
  }

  @NonNull
  @Override
  public DataType getType(@NonNull String name) {
    return DataTypes.INT;
  }

  @NonNull
  @Override
  public DataType getType(@NonNull CqlIdentifier id) {
    return DataTypes.INT;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return null;
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {}

  // equals and hashCode required for TCK tests that check that two subscribers
  // receive the exact same set of items.

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MockRow)) {
      return false;
    }
    MockRow mockRow = (MockRow) o;
    return index == mockRow.index;
  }

  @Override
  public int hashCode() {
    return index;
  }
}
