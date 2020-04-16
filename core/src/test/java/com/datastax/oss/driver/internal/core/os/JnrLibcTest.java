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
package com.datastax.oss.driver.internal.core.os;

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.Test;

/**
 * Explicitly test native impl based on jnr's POSIX impl. This test should pass on any platform
 * which is supported by jnr.
 */
public class JnrLibcTest {

  @Test
  public void should_be_available() {

    Libc impl = new JnrLibc();
    assertThat(impl.available()).isTrue();
  }

  @Test
  public void should_support_getpid() {
    Libc impl = new JnrLibc();
    Optional<Integer> val = impl.getpid();
    assertThat(val).isNotEmpty();
    assertThat(val.get()).isGreaterThan(1);
  }

  @Test
  public void should_support_gettimeofday() {
    Libc impl = new JnrLibc();
    Optional<Long> val = impl.gettimeofday();
    assertThat(val).isNotEmpty();
    assertThat(val.get()).isGreaterThan(0);

    Instant now = Instant.now();
    Instant rvInstant = Instant.EPOCH.plus(val.get(), ChronoUnit.MICROS);
    assertThat(rvInstant.isAfter(now.minusSeconds(1))).isTrue();
    assertThat(rvInstant.isBefore(now.plusSeconds(1))).isTrue();
  }
}
