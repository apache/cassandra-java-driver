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
package com.datastax.oss.driver.internal.core.channel;

import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class StreamIdGeneratorTest {
  @Test
  public void should_have_all_available_upon_creation() {
    StreamIdGenerator generator = new StreamIdGenerator(8);
    assertThat(generator.getAvailableIds()).isEqualTo(8);
  }

  @Test
  public void should_return_available_ids_in_sequence() {
    StreamIdGenerator generator = new StreamIdGenerator(8);
    for (int i = 0; i < 8; i++) {
      assertThat(generator.acquire()).isEqualTo(i);
      assertThat(generator.getAvailableIds()).isEqualTo(7 - i);
    }
  }

  @Test
  public void should_return_minus_one_when_no_id_available() {
    StreamIdGenerator generator = new StreamIdGenerator(8);
    for (int i = 0; i < 8; i++) {
      generator.acquire();
    }
    assertThat(generator.getAvailableIds()).isEqualTo(0);
    assertThat(generator.acquire()).isEqualTo(-1);
  }

  @Test
  public void should_return_previously_released_ids() {
    StreamIdGenerator generator = new StreamIdGenerator(8);
    for (int i = 0; i < 8; i++) {
      generator.acquire();
    }
    generator.release(7);
    generator.release(2);
    assertThat(generator.getAvailableIds()).isEqualTo(2);
    assertThat(generator.acquire()).isEqualTo(2);
    assertThat(generator.acquire()).isEqualTo(7);
    assertThat(generator.acquire()).isEqualTo(-1);
  }
}
