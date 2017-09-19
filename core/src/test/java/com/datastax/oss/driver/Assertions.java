/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.CassandraVersionAssert;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.CompletionStageAssert;
import com.datastax.oss.driver.internal.core.DriverConfigAssert;
import com.datastax.oss.driver.internal.core.NettyFutureAssert;
import com.datastax.oss.driver.internal.core.metadata.token.TokenRangeAssert;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletionStage;

public class Assertions extends org.assertj.core.api.Assertions {
  public static ByteBufAssert assertThat(ByteBuf actual) {
    return new ByteBufAssert(actual);
  }

  public static DriverConfigAssert assertThat(DriverConfig actual) {
    return new DriverConfigAssert(actual);
  }

  public static <V> NettyFutureAssert<V> assertThat(Future<V> actual) {
    return new NettyFutureAssert<>(actual);
  }

  public static <V> CompletionStageAssert<V> assertThat(CompletionStage<V> actual) {
    return new CompletionStageAssert<>(actual);
  }

  public static CassandraVersionAssert assertThat(CassandraVersion actual) {
    return new CassandraVersionAssert(actual);
  }

  public static TokenRangeAssert assertThat(TokenRange actual) {
    return new TokenRangeAssert(actual);
  }
}
