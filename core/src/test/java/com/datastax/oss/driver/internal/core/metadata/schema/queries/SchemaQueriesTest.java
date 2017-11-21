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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.google.common.collect.Iterators;
import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public abstract class SchemaQueriesTest {

  protected static final CqlIdentifier KS_ID = CqlIdentifier.fromInternal("ks");
  protected static final CqlIdentifier KS1_ID = CqlIdentifier.fromInternal("ks1");
  protected static final CqlIdentifier KS2_ID = CqlIdentifier.fromInternal("ks2");
  protected static final CqlIdentifier FOO_ID = CqlIdentifier.fromInternal("foo");

  @Mock protected Node node;
  @Mock protected DriverConfigProfile config;
  @Mock protected DriverChannel driverChannel;
  protected EmbeddedChannel channel;

  @Before
  public void setup() {
    // Whatever, not actually used because the requests are mocked
    Mockito.when(config.getDuration(CoreDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT))
        .thenReturn(Duration.ZERO);
    Mockito.when(config.getInt(CoreDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE))
        .thenReturn(5000);

    channel = new EmbeddedChannel();
    driverChannel = Mockito.mock(DriverChannel.class);
    Mockito.when(driverChannel.eventLoop()).thenReturn(channel.eventLoop());
  }

  protected static AdminRow mockRow(String... values) {
    AdminRow row = Mockito.mock(AdminRow.class);
    assertThat(values.length % 2).as("Expecting an even number of parameters").isZero();
    for (int i = 0; i < values.length / 2; i++) {
      Mockito.when(row.getString(values[i * 2])).thenReturn(values[i * 2 + 1]);
    }
    return row;
  }

  protected static AdminResult mockResult(AdminRow... rows) {
    return mockResult(null, rows);
  }

  protected static AdminResult mockResult(AdminResult next, AdminRow... rows) {
    AdminResult result = Mockito.mock(AdminResult.class);
    if (next == null) {
      Mockito.when(result.hasNextPage()).thenReturn(false);
    } else {
      Mockito.when(result.hasNextPage()).thenReturn(true);
      Mockito.when(result.nextPage()).thenReturn(CompletableFuture.completedFuture(next));
    }
    Mockito.when(result.iterator()).thenReturn(Iterators.forArray(rows));
    return result;
  }

  protected static class Call {
    final String query;
    final CompletableFuture<AdminResult> result;

    Call(String query) {
      this.query = query;
      this.result = new CompletableFuture<>();
    }
  }
}
