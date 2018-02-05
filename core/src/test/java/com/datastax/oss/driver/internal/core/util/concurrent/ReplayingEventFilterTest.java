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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class ReplayingEventFilterTest {
  private ReplayingEventFilter<Integer> filter;
  private List<Integer> filteredEvents;

  @Before
  public void setup() {
    filteredEvents = new ArrayList<>();
    filter = new ReplayingEventFilter<>(filteredEvents::add);
  }

  @Test
  public void should_discard_events_until_started() {
    filter.accept(1);
    filter.accept(2);
    assertThat(filteredEvents).isEmpty();
  }

  @Test
  public void should_accumulate_events_when_started() {
    filter.accept(1);
    filter.accept(2);
    filter.start();
    filter.accept(3);
    filter.accept(4);
    assertThat(filter.recordedEvents()).containsExactly(3, 4);
  }

  @Test
  public void should_flush_accumulated_events_when_ready() {
    filter.accept(1);
    filter.accept(2);
    filter.start();
    filter.accept(3);
    filter.accept(4);
    filter.markReady();
    assertThat(filteredEvents).containsExactly(3, 4);
    filter.accept(5);
    filter.accept(6);
    assertThat(filteredEvents).containsExactly(3, 4, 5, 6);
  }
}
