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
package com.datastax.oss.driver.internal.core.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.protocol.Lz4Compressor;
import com.datastax.oss.driver.internal.core.protocol.SnappyCompressor;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.NoopCompressor;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DefaultDriverContextTest {

  private DefaultDriverContext buildMockedContext(Optional<String> compressionOption) {

    DriverExecutionProfile defaultProfile = mock(DriverExecutionProfile.class);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn(compressionOption.orElse("none"));
    return MockedDriverContextFactory.defaultDriverContext(Optional.of(defaultProfile));
  }

  private void doCreateCompressorTest(Optional<String> configVal, Class<?> expectedClz) {

    DefaultDriverContext ctx = buildMockedContext(configVal);
    Compressor<ByteBuf> compressor = ctx.getCompressor();
    assertThat(compressor).isNotNull();
    assertThat(compressor).isInstanceOf(expectedClz);
  }

  @Test
  @DataProvider({"lz4", "lZ4", "Lz4", "LZ4"})
  public void should_create_lz4_compressor(String name) {

    doCreateCompressorTest(Optional.of(name), Lz4Compressor.class);
  }

  @Test
  @DataProvider({"snappy", "SNAPPY", "sNaPpY", "SNapPy"})
  public void should_create_snappy_compressor(String name) {

    doCreateCompressorTest(Optional.of(name), SnappyCompressor.class);
  }

  @Test
  public void should_create_noop_compressor_if_undefined() {

    doCreateCompressorTest(Optional.empty(), NoopCompressor.class);
  }

  @Test
  @DataProvider({"none", "NONE", "NoNe", "nONe"})
  public void should_create_noop_compressor_if_defined_as_none(String name) {

    doCreateCompressorTest(Optional.of(name), NoopCompressor.class);
  }
}
