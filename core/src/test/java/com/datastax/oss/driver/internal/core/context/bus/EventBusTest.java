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
package com.datastax.oss.driver.internal.core.context.bus;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.core.context.EventBus;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class EventBusTest {

  private EventBus bus;
  private Map<String, ParentEvent> results;
  private ChildEvent event = new ChildEvent();

  @Before
  public void setup() {
    bus = new EventBus("test");
    results = new HashMap<>();
  }

  @Test
  public void should_notify_registered_listeners() {
    // Given
    bus.register(ChildEvent.class, (e) -> results.put("listener1", e));
    bus.register(ChildEvent.class, (e) -> results.put("listener2", e));

    // When
    bus.fire(event);

    // Then
    assertThat(results)
        .hasSize(2)
        .containsEntry("listener1", event)
        .containsEntry("listener2", event);
  }

  @Test
  public void should_unregister_listener() {
    // Given
    Object key1 = bus.register(ChildEvent.class, (e) -> results.put("listener1", e));
    bus.register(ChildEvent.class, (e) -> results.put("listener2", e));
    bus.unregister(key1, ChildEvent.class);

    // When
    bus.fire(event);

    // Then
    assertThat(results).hasSize(1).containsEntry("listener2", event);
  }

  @Test
  public void should_use_exact_class() {
    // Given
    bus.register(ChildEvent.class, (e) -> results.put("listener1", e));
    bus.register(ParentEvent.class, (e) -> results.put("listener2", e));

    // When
    bus.fire(event);

    // Then
    assertThat(results).hasSize(1).containsEntry("listener1", event);

    // When
    results.clear();
    ParentEvent parentEvent = new ParentEvent();
    bus.fire(parentEvent);

    // Then
    assertThat(results).hasSize(1).containsEntry("listener2", parentEvent);
  }

  private static class ParentEvent {}

  private static class ChildEvent extends ParentEvent {}
}
