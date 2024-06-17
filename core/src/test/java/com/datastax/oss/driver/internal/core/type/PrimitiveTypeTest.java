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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class PrimitiveTypeTest {

  @Test
  public void should_report_protocol_code() {
    assertThat(DataTypes.ASCII.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.ASCII);
    assertThat(DataTypes.BIGINT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.BIGINT);
    assertThat(DataTypes.BLOB.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.BLOB);
    assertThat(DataTypes.BOOLEAN.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.BOOLEAN);
    assertThat(DataTypes.COUNTER.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.COUNTER);
    assertThat(DataTypes.DECIMAL.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.DECIMAL);
    assertThat(DataTypes.DOUBLE.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.DOUBLE);
    assertThat(DataTypes.FLOAT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.FLOAT);
    assertThat(DataTypes.INT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.INT);
    assertThat(DataTypes.TIMESTAMP.getProtocolCode())
        .isEqualTo(ProtocolConstants.DataType.TIMESTAMP);
    assertThat(DataTypes.UUID.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.UUID);
    assertThat(DataTypes.VARINT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.VARINT);
    assertThat(DataTypes.TIMEUUID.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.TIMEUUID);
    assertThat(DataTypes.INET.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.INET);
    assertThat(DataTypes.DATE.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.DATE);
    assertThat(DataTypes.TEXT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.VARCHAR);
    assertThat(DataTypes.TIME.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.TIME);
    assertThat(DataTypes.SMALLINT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.SMALLINT);
    assertThat(DataTypes.TINYINT.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.TINYINT);
    assertThat(DataTypes.DURATION.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.DURATION);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_format_as_cql(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(DataTypes.ASCII.asCql(true, true)).isEqualTo("ascii");
      assertThat(DataTypes.BIGINT.asCql(true, true)).isEqualTo("bigint");
      assertThat(DataTypes.BLOB.asCql(true, true)).isEqualTo("blob");
      assertThat(DataTypes.BOOLEAN.asCql(true, true)).isEqualTo("boolean");
      assertThat(DataTypes.COUNTER.asCql(true, true)).isEqualTo("counter");
      assertThat(DataTypes.DECIMAL.asCql(true, true)).isEqualTo("decimal");
      assertThat(DataTypes.DOUBLE.asCql(true, true)).isEqualTo("double");
      assertThat(DataTypes.FLOAT.asCql(true, true)).isEqualTo("float");
      assertThat(DataTypes.INT.asCql(true, true)).isEqualTo("int");
      assertThat(DataTypes.TIMESTAMP.asCql(true, true)).isEqualTo("timestamp");
      assertThat(DataTypes.UUID.asCql(true, true)).isEqualTo("uuid");
      assertThat(DataTypes.VARINT.asCql(true, true)).isEqualTo("varint");
      assertThat(DataTypes.TIMEUUID.asCql(true, true)).isEqualTo("timeuuid");
      assertThat(DataTypes.INET.asCql(true, true)).isEqualTo("inet");
      assertThat(DataTypes.DATE.asCql(true, true)).isEqualTo("date");
      assertThat(DataTypes.TEXT.asCql(true, true)).isEqualTo("text");
      assertThat(DataTypes.TIME.asCql(true, true)).isEqualTo("time");
      assertThat(DataTypes.SMALLINT.asCql(true, true)).isEqualTo("smallint");
      assertThat(DataTypes.TINYINT.asCql(true, true)).isEqualTo("tinyint");
      assertThat(DataTypes.DURATION.asCql(true, true)).isEqualTo("duration");
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_format_as_string(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(DataTypes.ASCII.toString()).isEqualTo("ASCII");
      assertThat(DataTypes.BIGINT.toString()).isEqualTo("BIGINT");
      assertThat(DataTypes.BLOB.toString()).isEqualTo("BLOB");
      assertThat(DataTypes.BOOLEAN.toString()).isEqualTo("BOOLEAN");
      assertThat(DataTypes.COUNTER.toString()).isEqualTo("COUNTER");
      assertThat(DataTypes.DECIMAL.toString()).isEqualTo("DECIMAL");
      assertThat(DataTypes.DOUBLE.toString()).isEqualTo("DOUBLE");
      assertThat(DataTypes.FLOAT.toString()).isEqualTo("FLOAT");
      assertThat(DataTypes.INT.toString()).isEqualTo("INT");
      assertThat(DataTypes.TIMESTAMP.toString()).isEqualTo("TIMESTAMP");
      assertThat(DataTypes.UUID.toString()).isEqualTo("UUID");
      assertThat(DataTypes.VARINT.toString()).isEqualTo("VARINT");
      assertThat(DataTypes.TIMEUUID.toString()).isEqualTo("TIMEUUID");
      assertThat(DataTypes.INET.toString()).isEqualTo("INET");
      assertThat(DataTypes.DATE.toString()).isEqualTo("DATE");
      assertThat(DataTypes.TEXT.toString()).isEqualTo("TEXT");
      assertThat(DataTypes.TIME.toString()).isEqualTo("TIME");
      assertThat(DataTypes.SMALLINT.toString()).isEqualTo("SMALLINT");
      assertThat(DataTypes.TINYINT.toString()).isEqualTo("TINYINT");
      assertThat(DataTypes.DURATION.toString()).isEqualTo("DURATION");
    } finally {
      Locale.setDefault(def);
    }
  }
}
