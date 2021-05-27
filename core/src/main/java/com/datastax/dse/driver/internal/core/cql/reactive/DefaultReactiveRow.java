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
package com.datastax.dse.driver.internal.core.cql.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
class DefaultReactiveRow implements ReactiveRow {

  private final Row row;
  private final ExecutionInfo executionInfo;

  DefaultReactiveRow(@NonNull Row row, @NonNull ExecutionInfo executionInfo) {
    this.row = row;
    this.executionInfo = executionInfo;
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return row.getColumnDefinitions();
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return row.getBytesUnsafe(i);
  }

  @Override
  public boolean isNull(int i) {
    return row.isNull(i);
  }

  @Override
  public <T> T get(int i, TypeCodec<T> codec) {
    return row.get(i, codec);
  }

  @Override
  public <T> T get(int i, GenericType<T> targetType) {
    return row.get(i, targetType);
  }

  @Override
  public <T> T get(int i, Class<T> targetClass) {
    return row.get(i, targetClass);
  }

  @Override
  public Object getObject(int i) {
    return row.getObject(i);
  }

  @Override
  public boolean getBoolean(int i) {
    return row.getBoolean(i);
  }

  @Override
  public byte getByte(int i) {
    return row.getByte(i);
  }

  @Override
  public double getDouble(int i) {
    return row.getDouble(i);
  }

  @Override
  public float getFloat(int i) {
    return row.getFloat(i);
  }

  @Override
  public int getInt(int i) {
    return row.getInt(i);
  }

  @Override
  public long getLong(int i) {
    return row.getLong(i);
  }

  @Override
  public short getShort(int i) {
    return row.getShort(i);
  }

  @Override
  public Instant getInstant(int i) {
    return row.getInstant(i);
  }

  @Override
  public LocalDate getLocalDate(int i) {
    return row.getLocalDate(i);
  }

  @Override
  public LocalTime getLocalTime(int i) {
    return row.getLocalTime(i);
  }

  @Override
  public ByteBuffer getByteBuffer(int i) {
    return row.getByteBuffer(i);
  }

  @Override
  public String getString(int i) {
    return row.getString(i);
  }

  @Override
  public BigInteger getBigInteger(int i) {
    return row.getBigInteger(i);
  }

  @Override
  public BigDecimal getBigDecimal(int i) {
    return row.getBigDecimal(i);
  }

  @Override
  public UUID getUuid(int i) {
    return row.getUuid(i);
  }

  @Override
  public InetAddress getInetAddress(int i) {
    return row.getInetAddress(i);
  }

  @Override
  public CqlDuration getCqlDuration(int i) {
    return row.getCqlDuration(i);
  }

  @Override
  public Token getToken(int i) {
    return row.getToken(i);
  }

  @Override
  public <T> List<T> getList(int i, @NonNull Class<T> elementsClass) {
    return row.getList(i, elementsClass);
  }

  @Override
  public <T> Set<T> getSet(int i, @NonNull Class<T> elementsClass) {
    return row.getSet(i, elementsClass);
  }

  @Override
  public <K, V> Map<K, V> getMap(int i, @NonNull Class<K> keyClass, @NonNull Class<V> valueClass) {
    return row.getMap(i, keyClass, valueClass);
  }

  @Override
  public UdtValue getUdtValue(int i) {
    return row.getUdtValue(i);
  }

  @Override
  public TupleValue getTupleValue(int i) {
    return row.getTupleValue(i);
  }

  @Override
  public int size() {
    return row.size();
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return row.getType(i);
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    return row.codecRegistry();
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    return row.protocolVersion();
  }

  @Override
  public ByteBuffer getBytesUnsafe(@NonNull String name) {
    return row.getBytesUnsafe(name);
  }

  @Override
  public boolean isNull(@NonNull String name) {
    return row.isNull(name);
  }

  @Override
  public <T> T get(@NonNull String name, @NonNull TypeCodec<T> codec) {
    return row.get(name, codec);
  }

  @Override
  public <T> T get(@NonNull String name, @NonNull GenericType<T> targetType) {
    return row.get(name, targetType);
  }

  @Override
  public <T> T get(@NonNull String name, @NonNull Class<T> targetClass) {
    return row.get(name, targetClass);
  }

  @Override
  public Object getObject(@NonNull String name) {
    return row.getObject(name);
  }

  @Override
  public boolean getBoolean(@NonNull String name) {
    return row.getBoolean(name);
  }

  @Override
  public byte getByte(@NonNull String name) {
    return row.getByte(name);
  }

  @Override
  public double getDouble(@NonNull String name) {
    return row.getDouble(name);
  }

  @Override
  public float getFloat(@NonNull String name) {
    return row.getFloat(name);
  }

  @Override
  public int getInt(@NonNull String name) {
    return row.getInt(name);
  }

  @Override
  public long getLong(@NonNull String name) {
    return row.getLong(name);
  }

  @Override
  public short getShort(@NonNull String name) {
    return row.getShort(name);
  }

  @Override
  public Instant getInstant(@NonNull String name) {
    return row.getInstant(name);
  }

  @Override
  public LocalDate getLocalDate(@NonNull String name) {
    return row.getLocalDate(name);
  }

  @Override
  public LocalTime getLocalTime(@NonNull String name) {
    return row.getLocalTime(name);
  }

  @Override
  public ByteBuffer getByteBuffer(@NonNull String name) {
    return row.getByteBuffer(name);
  }

  @Override
  public String getString(@NonNull String name) {
    return row.getString(name);
  }

  @Override
  public BigInteger getBigInteger(@NonNull String name) {
    return row.getBigInteger(name);
  }

  @Override
  public BigDecimal getBigDecimal(@NonNull String name) {
    return row.getBigDecimal(name);
  }

  @Override
  public UUID getUuid(@NonNull String name) {
    return row.getUuid(name);
  }

  @Override
  public InetAddress getInetAddress(@NonNull String name) {
    return row.getInetAddress(name);
  }

  @Override
  public CqlDuration getCqlDuration(@NonNull String name) {
    return row.getCqlDuration(name);
  }

  @Override
  public Token getToken(@NonNull String name) {
    return row.getToken(name);
  }

  @Override
  public <T> List<T> getList(@NonNull String name, @NonNull Class<T> elementsClass) {
    return row.getList(name, elementsClass);
  }

  @Override
  public <T> Set<T> getSet(@NonNull String name, @NonNull Class<T> elementsClass) {
    return row.getSet(name, elementsClass);
  }

  @Override
  public <K, V> Map<K, V> getMap(
      @NonNull String name, @NonNull Class<K> keyClass, @NonNull Class<V> valueClass) {
    return row.getMap(name, keyClass, valueClass);
  }

  @Override
  public UdtValue getUdtValue(@NonNull String name) {
    return row.getUdtValue(name);
  }

  @Override
  public TupleValue getTupleValue(@NonNull String name) {
    return row.getTupleValue(name);
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull String name) {
    return row.allIndicesOf(name);
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    return row.firstIndexOf(name);
  }

  @NonNull
  @Override
  public DataType getType(@NonNull String name) {
    return row.getType(name);
  }

  @Override
  public ByteBuffer getBytesUnsafe(@NonNull CqlIdentifier id) {
    return row.getBytesUnsafe(id);
  }

  @Override
  public boolean isNull(@NonNull CqlIdentifier id) {
    return row.isNull(id);
  }

  @Override
  public <T> T get(@NonNull CqlIdentifier id, @NonNull TypeCodec<T> codec) {
    return row.get(id, codec);
  }

  @Override
  public <T> T get(@NonNull CqlIdentifier id, @NonNull GenericType<T> targetType) {
    return row.get(id, targetType);
  }

  @Override
  public <T> T get(@NonNull CqlIdentifier id, @NonNull Class<T> targetClass) {
    return row.get(id, targetClass);
  }

  @Override
  public Object getObject(@NonNull CqlIdentifier id) {
    return row.getObject(id);
  }

  @Override
  public boolean getBoolean(@NonNull CqlIdentifier id) {
    return row.getBoolean(id);
  }

  @Override
  public byte getByte(@NonNull CqlIdentifier id) {
    return row.getByte(id);
  }

  @Override
  public double getDouble(@NonNull CqlIdentifier id) {
    return row.getDouble(id);
  }

  @Override
  public float getFloat(@NonNull CqlIdentifier id) {
    return row.getFloat(id);
  }

  @Override
  public int getInt(@NonNull CqlIdentifier id) {
    return row.getInt(id);
  }

  @Override
  public long getLong(@NonNull CqlIdentifier id) {
    return row.getLong(id);
  }

  @Override
  public short getShort(@NonNull CqlIdentifier id) {
    return row.getShort(id);
  }

  @Override
  public Instant getInstant(@NonNull CqlIdentifier id) {
    return row.getInstant(id);
  }

  @Override
  public LocalDate getLocalDate(@NonNull CqlIdentifier id) {
    return row.getLocalDate(id);
  }

  @Override
  public LocalTime getLocalTime(@NonNull CqlIdentifier id) {
    return row.getLocalTime(id);
  }

  @Override
  public ByteBuffer getByteBuffer(@NonNull CqlIdentifier id) {
    return row.getByteBuffer(id);
  }

  @Override
  public String getString(@NonNull CqlIdentifier id) {
    return row.getString(id);
  }

  @Override
  public BigInteger getBigInteger(@NonNull CqlIdentifier id) {
    return row.getBigInteger(id);
  }

  @Override
  public BigDecimal getBigDecimal(@NonNull CqlIdentifier id) {
    return row.getBigDecimal(id);
  }

  @Override
  public UUID getUuid(@NonNull CqlIdentifier id) {
    return row.getUuid(id);
  }

  @Override
  public InetAddress getInetAddress(@NonNull CqlIdentifier id) {
    return row.getInetAddress(id);
  }

  @Override
  public CqlDuration getCqlDuration(@NonNull CqlIdentifier id) {
    return row.getCqlDuration(id);
  }

  @Override
  public Token getToken(@NonNull CqlIdentifier id) {
    return row.getToken(id);
  }

  @Override
  public <T> List<T> getList(@NonNull CqlIdentifier id, @NonNull Class<T> elementsClass) {
    return row.getList(id, elementsClass);
  }

  @Override
  public <T> Set<T> getSet(@NonNull CqlIdentifier id, @NonNull Class<T> elementsClass) {
    return row.getSet(id, elementsClass);
  }

  @Override
  public <K, V> Map<K, V> getMap(
      @NonNull CqlIdentifier id, @NonNull Class<K> keyClass, @NonNull Class<V> valueClass) {
    return row.getMap(id, keyClass, valueClass);
  }

  @Override
  public UdtValue getUdtValue(@NonNull CqlIdentifier id) {
    return row.getUdtValue(id);
  }

  @Override
  public TupleValue getTupleValue(@NonNull CqlIdentifier id) {
    return row.getTupleValue(id);
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    return row.allIndicesOf(id);
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    return row.firstIndexOf(id);
  }

  @NonNull
  @Override
  public DataType getType(@NonNull CqlIdentifier id) {
    return row.getType(id);
  }

  @Override
  public boolean isDetached() {
    return row.isDetached();
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    row.attach(attachmentPoint);
  }

  @Override
  public String toString() {
    return "DefaultReactiveRow{row=" + row + ", executionInfo=" + executionInfo + '}';
  }
}
