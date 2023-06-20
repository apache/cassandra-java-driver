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
package com.datastax.oss.driver.api.core.uuid;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class UuidsTest {

  @Test
  public void should_generate_unique_random_uuids_Random() {
    Set<UUID> generated = serialGeneration(1_000_000, Uuids::random);
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_random_uuids_shared_Random2() {
    Random random = new Random();
    Set<UUID> generated = serialGeneration(1_000_000, () -> Uuids.random(random));
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_random_uuids_across_threads_shared_Random() throws Exception {
    Random random = new Random();
    Set<UUID> generated = parallelGeneration(10, 10_000, () -> () -> Uuids.random(random));
    assertThat(generated).hasSize(10 * 10_000);
  }

  @Test
  public void should_generate_unique_random_uuids_shared_SecureRandom() {
    SecureRandom random = new SecureRandom();
    Set<UUID> generated = serialGeneration(1_000_000, () -> Uuids.random(random));
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_random_uuids_across_threads_shared_SecureRandom()
      throws Exception {
    SecureRandom random = new SecureRandom();
    Set<UUID> generated = parallelGeneration(10, 10_000, () -> () -> Uuids.random(random));
    assertThat(generated).hasSize(10 * 10_000);
  }

  @Test
  public void should_generate_unique_random_uuids_ThreadLocalRandom() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    Set<UUID> generated = serialGeneration(1_000_000, () -> Uuids.random(random));
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_random_uuids_across_threads_ThreadLocalRandom()
      throws Exception {
    Set<UUID> generated =
        parallelGeneration(
            10,
            10_000,
            () -> {
              ThreadLocalRandom random = ThreadLocalRandom.current();
              return () -> Uuids.random(random);
            });
    assertThat(generated).hasSize(10 * 10_000);
  }

  @Test
  public void should_generate_unique_random_uuids_Netty_ThreadLocalRandom() {
    io.netty.util.internal.ThreadLocalRandom random =
        io.netty.util.internal.ThreadLocalRandom.current();
    Set<UUID> generated = serialGeneration(1_000_000, () -> Uuids.random(random));
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_random_uuids_across_threads_Netty_ThreadLocalRandom()
      throws Exception {
    Set<UUID> generated =
        parallelGeneration(
            10,
            10_000,
            () -> {
              io.netty.util.internal.ThreadLocalRandom random =
                  io.netty.util.internal.ThreadLocalRandom.current();
              return () -> Uuids.random(random);
            });
    assertThat(generated).hasSize(10 * 10_000);
  }

  @Test
  public void should_generate_unique_random_uuids_SplittableRandom() {
    SplittableRandom random = new SplittableRandom();
    Set<UUID> generated = serialGeneration(1_000_000, () -> Uuids.random(random));
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_random_uuids_across_threads_SplittableRandom()
      throws Exception {
    Set<UUID> generated =
        parallelGeneration(
            10,
            10_000,
            () -> {
              SplittableRandom random = new SplittableRandom();
              return () -> Uuids.random(random);
            });
    assertThat(generated).hasSize(10 * 10_000);
  }

  @Test
  @UseDataProvider("byteArrayNames")
  public void should_generate_name_based_uuid_from_namespace_and_byte_array(
      UUID namespace, byte[] name) throws NoSuchAlgorithmException {
    // when
    UUID actual = Uuids.nameBased(namespace, name);
    // then
    assertThat(actual).isNotNull();
    assertThat(actual.version()).isEqualTo(3);
    assertUuid(namespace, name, 3, actual);
  }

  @DataProvider
  public static Object[][] byteArrayNames() {
    return new Object[][] {
      {Uuids.NAMESPACE_DNS, new byte[] {}}, {Uuids.NAMESPACE_URL, new byte[] {1, 2, 3, 4}},
    };
  }

  @Test
  @UseDataProvider("byteArrayNamesWithVersions")
  public void should_generate_name_based_uuid_from_namespace_byte_array_and_version(
      UUID namespace, byte[] name, int version) throws NoSuchAlgorithmException {
    // when
    UUID actual = Uuids.nameBased(namespace, name, version);
    // then
    assertThat(actual).isNotNull();
    assertThat(actual.version()).isEqualTo(version);
    assertUuid(namespace, name, version, actual);
  }

  @DataProvider
  public static Object[][] byteArrayNamesWithVersions() {
    return new Object[][] {
      {Uuids.NAMESPACE_DNS, new byte[] {}, 3},
      {Uuids.NAMESPACE_URL, new byte[] {1, 2, 3, 4}, 3},
      {Uuids.NAMESPACE_OID, new byte[] {}, 5},
      {Uuids.NAMESPACE_X500, new byte[] {1, 2, 3, 4}, 5},
    };
  }

  @Test
  @UseDataProvider("stringNames")
  public void should_generate_name_based_uuid_from_namespace_and_string(UUID namespace, String name)
      throws NoSuchAlgorithmException {
    // when
    UUID actual = Uuids.nameBased(namespace, name);
    // then
    assertThat(actual).isNotNull();
    assertThat(actual.version()).isEqualTo(3);
    assertUuid(namespace, name, 3, actual);
  }

  @DataProvider
  public static Object[][] stringNames() {
    return new Object[][] {
      {Uuids.NAMESPACE_DNS, ""}, {Uuids.NAMESPACE_URL, "Hello world!"}, {Uuids.NAMESPACE_OID, "你好"},
    };
  }

  @Test
  @UseDataProvider("stringNamesWithVersions")
  public void should_generate_name_based_uuid_from_namespace_string_and_version(
      UUID namespace, String name, int version) throws NoSuchAlgorithmException {
    // when
    UUID actual = Uuids.nameBased(namespace, name, version);
    // then
    assertThat(actual).isNotNull();
    assertThat(actual.version()).isEqualTo(version);
    assertUuid(namespace, name, version, actual);
  }

  @DataProvider
  public static Object[][] stringNamesWithVersions() {
    return new Object[][] {
      {Uuids.NAMESPACE_DNS, "", 3},
      {Uuids.NAMESPACE_URL, "Hello world!", 3},
      {Uuids.NAMESPACE_OID, "你好", 3},
      {Uuids.NAMESPACE_DNS, "", 5},
      {Uuids.NAMESPACE_URL, "Hello world!", 5},
      {Uuids.NAMESPACE_OID, "你好", 5},
    };
  }

  @Test
  @UseDataProvider("concatenatedData")
  public void should_generate_name_based_uuid_from_concatenated_data(byte[] namespaceAndName)
      throws NoSuchAlgorithmException {
    // when
    UUID actual = Uuids.nameBased(namespaceAndName);
    // then
    assertThat(actual).isNotNull();
    assertThat(actual.version()).isEqualTo(3);
    assertUuid(namespaceAndName, 3, actual);
  }

  @DataProvider
  public static Object[][] concatenatedData() {
    return new Object[][] {
      {concat(Uuids.NAMESPACE_DNS, new byte[] {})},
      {concat(Uuids.NAMESPACE_URL, new byte[] {1, 2, 3, 4})},
    };
  }

  @Test
  @UseDataProvider("concatenatedDataWithVersions")
  public void should_generate_name_based_uuid_from_concatenated_data_and_version(
      byte[] namespaceAndName, int version) throws NoSuchAlgorithmException {
    // when
    UUID actual = Uuids.nameBased(namespaceAndName, version);
    // then
    assertThat(actual).isNotNull();
    assertThat(actual.version()).isEqualTo(version);
    assertUuid(namespaceAndName, version, actual);
  }

  @DataProvider
  public static Object[][] concatenatedDataWithVersions() {
    return new Object[][] {
      {concat(Uuids.NAMESPACE_DNS, new byte[] {}), 3},
      {concat(Uuids.NAMESPACE_URL, new byte[] {1, 2, 3, 4}), 3},
      {concat(Uuids.NAMESPACE_DNS, new byte[] {}), 5},
      {concat(Uuids.NAMESPACE_URL, new byte[] {1, 2, 3, 4}), 5},
    };
  }

  @Test
  public void should_throw_when_invalid_version() {
    Throwable error = catchThrowable(() -> Uuids.nameBased(Uuids.NAMESPACE_URL, "irrelevant", 1));
    assertThat(error)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid name-based UUID version, expecting 3 or 5, got: 1");
  }

  @Test
  public void should_throw_when_invalid_data() {
    Throwable error = catchThrowable(() -> Uuids.nameBased(new byte[] {1}, 3));
    assertThat(error)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("namespaceAndName must be at least 16 bytes long");
  }

  @Test
  public void should_generate_timestamp_within_10_ms() {

    // The Uuids class does some computation at class initialization, which may screw up our
    // assumption below that Uuids.timeBased() takes less than 10ms, so force class loading now.
    Uuids.timeBased();

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
    Set<UUID> generated = serialGeneration(1_000_000, Uuids::timeBased);
    assertThat(generated).hasSize(1_000_000);
  }

  @Test
  public void should_generate_unique_time_based_uuids_across_threads() throws Exception {
    Set<UUID> generated = parallelGeneration(10, 10_000, () -> Uuids::timeBased);
    assertThat(generated).hasSize(10 * 10_000);
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
      long timestamp = random.nextInt();
      for (int j = 0; j < uuidsPerTimestamp; j++) {
        UUID uuid = new UUID(Uuids.makeMsb(Uuids.fromUnixTimestamp(timestamp)), random.nextLong());
        assertBetween(uuid, Uuids.startOf(timestamp), Uuids.endOf(timestamp));
      }
    }
  }

  // Compares using Cassandra's sorting algorithm (not the same as compareTo).
  private static void assertBetween(UUID uuid, UUID lowerBound, UUID upperBound) {
    ByteBuffer uuidBytes = TypeCodecs.UUID.encode(uuid, DefaultProtocolVersion.V3);
    ByteBuffer lb = TypeCodecs.UUID.encode(lowerBound, DefaultProtocolVersion.V3);
    ByteBuffer ub = TypeCodecs.UUID.encode(upperBound, DefaultProtocolVersion.V3);
    assertThat(uuidBytes).isNotNull();
    assertThat(lb).isNotNull();
    assertThat(ub).isNotNull();
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

  private static void assertUuid(UUID namespace, String name, int version, UUID actual)
      throws NoSuchAlgorithmException {
    assertUuid(namespace, name.getBytes(StandardCharsets.UTF_8), version, actual);
  }

  private static void assertUuid(UUID namespace, byte[] name, int version, UUID actual)
      throws NoSuchAlgorithmException {
    byte[] data = digest(namespace, name, version);
    assertThat(longToBytes(actual.getMostSignificantBits()))
        .isEqualTo(Arrays.copyOfRange(data, 0, 8));
    assertThat(longToBytes(actual.getLeastSignificantBits()))
        .isEqualTo(Arrays.copyOfRange(data, 8, 16));
  }

  private static void assertUuid(byte[] namespaceAndName, int version, UUID actual)
      throws NoSuchAlgorithmException {
    byte[] data = digest(namespaceAndName, version);
    assertThat(longToBytes(actual.getMostSignificantBits()))
        .isEqualTo(Arrays.copyOfRange(data, 0, 8));
    assertThat(longToBytes(actual.getLeastSignificantBits()))
        .isEqualTo(Arrays.copyOfRange(data, 8, 16));
  }

  private static byte[] digest(UUID namespace, byte[] name, int version)
      throws NoSuchAlgorithmException {
    byte[] namespaceAndName = concat(namespace, name);
    return digest(namespaceAndName, version);
  }

  private static byte[] digest(byte[] namespaceAndName, int version)
      throws NoSuchAlgorithmException {
    MessageDigest result;
    String algorithm = version == 3 ? "MD5" : "SHA-1";
    result = MessageDigest.getInstance(algorithm);
    byte[] digest = result.digest(namespaceAndName);
    digest[6] &= (byte) 0x0f;
    digest[6] |= (byte) (version << 4);
    digest[8] &= (byte) 0x3f;
    digest[8] |= (byte) 0x80;
    return digest;
  }

  private static byte[] concat(UUID namespace, byte[] name) {
    return ByteBuffer.allocate(16 + name.length)
        .putLong(namespace.getMostSignificantBits())
        .putLong(namespace.getLeastSignificantBits())
        .put(name)
        .array();
  }

  private static byte[] longToBytes(long x) {
    return ByteBuffer.allocate(Long.BYTES).putLong(x).array();
  }

  private Set<UUID> serialGeneration(int count, Supplier<UUID> uuidSupplier) {
    Set<UUID> generated = new HashSet<>(count);
    for (int i = 0; i < count; ++i) {
      generated.add(uuidSupplier.get());
    }
    return generated;
  }

  public Set<UUID> parallelGeneration(
      int threadCount, int uuidsPerThread, Supplier<Supplier<UUID>> uuidSupplier)
      throws InterruptedException {
    Set<UUID> generated = new ConcurrentSkipListSet<>();
    UuidGenerator[] generators = new UuidGenerator[threadCount];
    for (int i = 0; i < threadCount; i++) {
      generators[i] = new UuidGenerator(uuidsPerThread, uuidSupplier, generated);
    }
    for (int i = 0; i < threadCount; i++) {
      generators[i].start();
    }
    for (int i = 0; i < threadCount; i++) {
      generators[i].join();
    }
    return generated;
  }

  private static class UuidGenerator extends Thread {

    private final int toGenerate;
    private final Set<UUID> generated;
    private final Supplier<Supplier<UUID>> uuidSupplier;

    UuidGenerator(int toGenerate, Supplier<Supplier<UUID>> uuidSupplier, Set<UUID> generated) {
      this.toGenerate = toGenerate;
      this.generated = generated;
      this.uuidSupplier = uuidSupplier;
    }

    @Override
    public void run() {
      Supplier<UUID> uuidSupplier = this.uuidSupplier.get();
      for (int i = 0; i < toGenerate; ++i) {
        generated.add(uuidSupplier.get());
      }
    }
  }
}
