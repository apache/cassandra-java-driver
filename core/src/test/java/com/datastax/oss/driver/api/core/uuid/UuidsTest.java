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
package com.datastax.oss.driver.api.core.uuid;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class UuidsTest {

  @Test
  public void should_generate_timestamp_within_10_ms() {

    // The Uuids class does some computation at class initialization, which may screw up our
    // assumption below that Uuids.timeBased() takes less than 10ms, so force class loading now.
    Uuids.random();

    long start = System.currentTimeMillis();
    UUID uuid = Uuids.timeBased();

    assertThat(uuid.version()).isEqualTo(1);
    assertThat(uuid.variant()).isEqualTo(2);

    long timestamp = Uuids.unixTimestamp(uuid);

    assertThat(timestamp)
        .as("Generated timestamp should be within 10 ms")
        .isBetween(start, start + 10);
  }

  @Test
  public void should_generate_unique_time_based_uuids() {
    int count = 1_000_000;
    Set<UUID> generated = new HashSet<>(count);

    for (int i = 0; i < count; ++i) {
      generated.add(Uuids.timeBased());
    }

    assertThat(generated).hasSize(count);
  }

  @Test
  public void should_generate_unique_time_based_uuids_across_threads() throws Exception {
    int threadCount = 10;
    int uuidsPerThread = 10_000;
    Set<UUID> generated = new ConcurrentSkipListSet<>();

    UUIDGenerator[] generators = new UUIDGenerator[threadCount];
    for (int i = 0; i < threadCount; i++) {
      generators[i] = new UUIDGenerator(uuidsPerThread, generated);
    }
    for (int i = 0; i < threadCount; i++) {
      generators[i].start();
    }
    for (int i = 0; i < threadCount; i++) {
      generators[i].join();
    }

    assertThat(generated).hasSize(threadCount * uuidsPerThread);
  }

  @Test
  public void should_generate_ever_increasing_timestamps() {
    int count = 1_000_000;
    long previous = 0;
    for (int i = 0; i < count; i++) {
      long current = Uuids.timeBased().timestamp();
      assertThat(current).isGreaterThan(previous);
      previous = current;
    }
  }

  @Test
  public void should_generate_within_bounds_for_given_timestamp() {

    Random random = new Random(System.currentTimeMillis());

    int timestampsCount = 10;
    int uuidsPerTimestamp = 10;

    for (int i = 0; i < timestampsCount; i++) {
      long timestamp = (long) random.nextInt();
      for (int j = 0; j < uuidsPerTimestamp; j++) {
        UUID uuid = new UUID(Uuids.makeMsb(Uuids.fromUnixTimestamp(timestamp)), random.nextLong());
        assertBetween(uuid, Uuids.startOf(timestamp), Uuids.endOf(timestamp));
      }
    }
  }

  // Compares using Cassandra's sorting algorithm (not the same as compareTo).
  private static void assertBetween(UUID uuid, UUID lowerBound, UUID upperBound) {
    ByteBuffer uuidBytes = TypeCodecs.UUID.encode(uuid, CoreProtocolVersion.V3);
    ByteBuffer lb = TypeCodecs.UUID.encode(lowerBound, CoreProtocolVersion.V3);
    ByteBuffer ub = TypeCodecs.UUID.encode(upperBound, CoreProtocolVersion.V3);
    assertThat(compareTimestampBytes(lb, uuidBytes)).isLessThanOrEqualTo(0);
    assertThat(compareTimestampBytes(ub, uuidBytes)).isGreaterThanOrEqualTo(0);
  }

  private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2) {
    int o1Pos = o1.position();
    int o2Pos = o2.position();

    int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
    if (d != 0) {
      return d;
    }
    d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
    if (d != 0) {
      return d;
    }
    d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
    if (d != 0) {
      return d;
    }
    d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
    if (d != 0) {
      return d;
    }
    d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
    if (d != 0) {
      return d;
    }
    d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
    if (d != 0) {
      return d;
    }
    d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
    if (d != 0) {
      return d;
    }
    return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
  }

  private static class UUIDGenerator extends Thread {

    private final int toGenerate;
    private final Set<UUID> generated;

    UUIDGenerator(int toGenerate, Set<UUID> generated) {
      this.toGenerate = toGenerate;
      this.generated = generated;
    }

    @Override
    public void run() {
      for (int i = 0; i < toGenerate; ++i) {
        generated.add(Uuids.timeBased());
      }
    }
  }
}
