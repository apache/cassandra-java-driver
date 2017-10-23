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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;

public class ChannelSetTest {
  @Mock private DriverChannel channel1, channel2, channel3;
  private ChannelSet set;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    set = new ChannelSet();
  }

  @Test
  public void should_return_null_when_empty() {
    assertThat(set.size()).isEqualTo(0);
    assertThat(set.next()).isNull();
  }

  @Test
  public void should_return_element_when_single() {
    // When
    set.add(channel1);

    // Then
    assertThat(set.size()).isEqualTo(1);
    assertThat(set.next()).isEqualTo(channel1);
    Mockito.verify(channel1, never()).getAvailableIds();
  }

  @Test
  public void should_return_most_available_when_multiple() {
    // Given
    Mockito.when(channel1.getAvailableIds()).thenReturn(2);
    Mockito.when(channel2.getAvailableIds()).thenReturn(12);
    Mockito.when(channel3.getAvailableIds()).thenReturn(8);

    // When
    set.add(channel1);
    set.add(channel2);
    set.add(channel3);

    // Then
    assertThat(set.size()).isEqualTo(3);
    assertThat(set.next()).isEqualTo(channel2);
    Mockito.verify(channel1).getAvailableIds();
    Mockito.verify(channel2).getAvailableIds();
    Mockito.verify(channel3).getAvailableIds();

    // When
    Mockito.when(channel1.getAvailableIds()).thenReturn(15);

    // Then
    assertThat(set.next()).isEqualTo(channel1);
  }

  @Test
  public void should_remove_channels() {
    // Given
    Mockito.when(channel1.getAvailableIds()).thenReturn(2);
    Mockito.when(channel2.getAvailableIds()).thenReturn(12);
    Mockito.when(channel3.getAvailableIds()).thenReturn(8);

    set.add(channel1);
    set.add(channel2);
    set.add(channel3);
    assertThat(set.next()).isEqualTo(channel2);

    // When
    set.remove(channel2);

    // Then
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.next()).isEqualTo(channel3);

    // When
    set.remove(channel3);

    // Then
    assertThat(set.size()).isEqualTo(1);
    assertThat(set.next()).isEqualTo(channel1);

    // When
    set.remove(channel1);

    // Then
    assertThat(set.size()).isEqualTo(0);
    assertThat(set.next()).isNull();
  }
}
