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
package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.protocol.internal.NoopCompressor;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class BuiltInCompressorsTest {

  @Mock private DriverContext context;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_create_instance_for_supported_algorithms(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(BuiltInCompressors.newInstance("lz4", context)).isInstanceOf(Lz4Compressor.class);
      assertThat(BuiltInCompressors.newInstance("snappy", context))
          .isInstanceOf(SnappyCompressor.class);
      assertThat(BuiltInCompressors.newInstance("none", context))
          .isInstanceOf(NoopCompressor.class);
      assertThat(BuiltInCompressors.newInstance("LZ4", context)).isInstanceOf(Lz4Compressor.class);
      assertThat(BuiltInCompressors.newInstance("SNAPPY", context))
          .isInstanceOf(SnappyCompressor.class);
      assertThat(BuiltInCompressors.newInstance("NONE", context))
          .isInstanceOf(NoopCompressor.class);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  public void should_throw_when_unsupported_algorithm() {
    assertThatThrownBy(() -> BuiltInCompressors.newInstance("GZIP", context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported compression algorithm 'GZIP'");
  }
}
