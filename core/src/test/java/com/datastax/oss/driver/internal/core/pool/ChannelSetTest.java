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

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
    // Given
    when(channel1.preAcquireId()).thenReturn(true);

    // When
    set.add(channel1);

    // Then
    assertThat(set.size()).isEqualTo(1);
    assertThat(set.next()).isEqualTo(channel1);
    verify(channel1, never()).getAvailableIds();
    verify(channel1).preAcquireId();
  }

  @Test
  public void should_return_null_when_single_but_full() {
    // Given
    when(channel1.preAcquireId()).thenReturn(false);

    // When
    set.add(channel1);

    // Then
    assertThat(set.next()).isNull();
    verify(channel1).preAcquireId();
  }

  @Test
  public void should_return_most_available_when_multiple() {
    // Given
    when(channel1.getAvailableIds()).thenReturn(2);
    when(channel2.getAvailableIds()).thenReturn(12);
    when(channel3.getAvailableIds()).thenReturn(8);
    when(channel2.preAcquireId()).thenReturn(true);

    // When
    set.add(channel1);
    set.add(channel2);
    set.add(channel3);

    // Then
    assertThat(set.size()).isEqualTo(3);
    assertThat(set.next()).isEqualTo(channel2);
    verify(channel1).getAvailableIds();
    verify(channel2).getAvailableIds();
    verify(channel3).getAvailableIds();
    verify(channel2).preAcquireId();

    // When
    when(channel1.getAvailableIds()).thenReturn(15);
    when(channel1.preAcquireId()).thenReturn(true);

    // Then
    assertThat(set.next()).isEqualTo(channel1);
    verify(channel1).preAcquireId();
  }

  @Test
  public void should_return_null_when_multiple_but_all_full() {
    // Given
    when(channel1.getAvailableIds()).thenReturn(0);
    when(channel2.getAvailableIds()).thenReturn(0);
    when(channel3.getAvailableIds()).thenReturn(0);

    // When
    set.add(channel1);
    set.add(channel2);
    set.add(channel3);

    // Then
    assertThat(set.next()).isNull();
  }

  @Test
  public void should_remove_channels() {
    // Given
    when(channel1.getAvailableIds()).thenReturn(2);
    when(channel2.getAvailableIds()).thenReturn(12);
    when(channel3.getAvailableIds()).thenReturn(8);
    when(channel2.preAcquireId()).thenReturn(true);

    set.add(channel1);
    set.add(channel2);
    set.add(channel3);
    assertThat(set.next()).isEqualTo(channel2);

    // When
    set.remove(channel2);
    when(channel3.preAcquireId()).thenReturn(true);

    // Then
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.next()).isEqualTo(channel3);

    // When
    set.remove(channel3);
    when(channel1.preAcquireId()).thenReturn(true);

    // Then
    assertThat(set.size()).isEqualTo(1);
    assertThat(set.next()).isEqualTo(channel1);

    // When
    set.remove(channel1);

    // Then
    assertThat(set.size()).isEqualTo(0);
    assertThat(set.next()).isNull();
  }

  /**
   * Check that {@link ChannelSet#next()} doesn't spin forever if it keeps racing (see comments in
   * the implementation).
   */
  @Test
  public void should_not_loop_indefinitely_if_acquisition_keeps_failing() {
    // Given
    when(channel1.getAvailableIds()).thenReturn(2);
    when(channel2.getAvailableIds()).thenReturn(12);
    when(channel3.getAvailableIds()).thenReturn(8);
    // channel2 is the most available but we keep failing to acquire (simulating the race condition)
    when(channel2.preAcquireId()).thenReturn(false);

    // When
    set.add(channel1);
    set.add(channel2);
    set.add(channel3);

    // Then
    assertThat(set.next()).isNull();
  }
}
