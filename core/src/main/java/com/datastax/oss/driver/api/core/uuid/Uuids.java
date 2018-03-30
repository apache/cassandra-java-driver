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

import com.datastax.oss.driver.internal.core.os.Native;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to help working with UUIDs, and more specifically, with time-based UUIDs (also
 * known as Version 1 UUIDs).
 *
 * <p>The algorithm to generate time-based UUIDs roughly follows the description in RFC-4122, but
 * with the following adaptations:
 *
 * <ol>
 *   <li>Since Java does not provide direct access to the host's MAC address, that information is
 *       replaced with a digest of all IP addresses available on the host;
 *   <li>The process ID (PID) isn't easily available to Java either, so it is determined by one of
 *       the following methods, in the order they are listed below:
 *       <ol>
 *         <li>If the System property <code>{@value PID_SYSTEM_PROPERTY}</code> is set then the
 *             value to use as a PID will be read from that property;
 *         <li>Otherwise, if a native call to {@code POSIX.getpid()} is possible, then the PID will
 *             be read from that call;
 *         <li>Otherwise, an attempt will be made to read the PID from JMX's {@link
 *             ManagementFactory#getRuntimeMXBean() RuntimeMXBean}, since most JVMs tend to use the
 *             JVM's PID as part of that MXBean name (however that behavior is not officially part
 *             of the specification, so it may not work for all JVMs);
 *         <li>If all of the above fail, a random integer will be generated and used as a surrogate
 *             PID.
 *       </ol>
 * </ol>
 *
 * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-444">JAVA-444</a>
 * @see <a href="http://www.ietf.org/rfc/rfc4122.txt">A Universally Unique IDentifier (UUID) URN
 *     Namespace (RFC 4122)</a>
 */
public final class Uuids {

  /** The system property to use to force the value of the process ID ({@value}). */
  public static final String PID_SYSTEM_PROPERTY = "com.datastax.oss.driver.PID";

  private static final Logger LOG = LoggerFactory.getLogger(Uuids.class);

  private Uuids() {}

  private static final long START_EPOCH = makeEpoch();
  private static final long CLOCK_SEQ_AND_NODE = makeClockSeqAndNode();

  // The min and max possible lsb for a UUID.
  //
  // This is not 0 and all 1's because Cassandra's TimeUUIDType compares the lsb parts as signed
  // byte arrays. So the min value is 8 times -128 and the max is 8 times +127.
  //
  // We ignore the UUID variant (namely, MIN_CLOCK_SEQ_AND_NODE has variant 2 as it should, but
  // MAX_CLOCK_SEQ_AND_NODE has variant 0) because I don't trust all UUID implementations to have
  // correctly set those (pycassa doesn't always for instance).
  private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
  private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

  private static final AtomicLong lastTimestamp = new AtomicLong(0L);

  private static long makeEpoch() {
    // UUID v1 timestamps must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582.
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
    c.set(Calendar.YEAR, 1582);
    c.set(Calendar.MONTH, Calendar.OCTOBER);
    c.set(Calendar.DAY_OF_MONTH, 15);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTimeInMillis();
  }

  private static long makeNode() {

    // We don't have access to the MAC address (in pure JAVA at least) but need to generate a node
    // part that identifies this host as uniquely as possible.
    // The spec says that one option is to take as many sources that identify this node as possible
    // and hash them together. That's what we do here by gathering all the IPs of this host as well
    // as a few other sources.
    try {

      MessageDigest digest = MessageDigest.getInstance("MD5");
      for (String address : getAllLocalAddresses()) update(digest, address);

      Properties props = System.getProperties();
      update(digest, props.getProperty("java.vendor"));
      update(digest, props.getProperty("java.vendor.url"));
      update(digest, props.getProperty("java.version"));
      update(digest, props.getProperty("os.arch"));
      update(digest, props.getProperty("os.name"));
      update(digest, props.getProperty("os.version"));
      update(digest, getProcessPiece());

      byte[] hash = digest.digest();

      long node = 0;
      for (int i = 0; i < 6; i++) node |= (0x00000000000000ffL & (long) hash[i]) << (i * 8);
      // Since we don't use the MAC address, the spec says that the multicast bit (least significant
      // bit of the first byte of the node ID) must be 1.
      return node | 0x0000010000000000L;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getProcessPiece() {
    Integer pid = null;
    String pidProperty = System.getProperty(PID_SYSTEM_PROPERTY);
    if (pidProperty != null) {
      try {
        pid = Integer.parseInt(pidProperty);
        LOG.info("PID obtained from System property {}: {}", PID_SYSTEM_PROPERTY, pid);
      } catch (NumberFormatException e) {
        LOG.warn(
            "Incorrect integer specified for PID in System property {}: {}",
            PID_SYSTEM_PROPERTY,
            pidProperty);
      }
    }
    if (pid == null && Native.isGetProcessIdAvailable()) {
      try {
        pid = Native.getProcessId();
        LOG.info("PID obtained through native call to getpid(): {}", pid);
      } catch (Exception e) {
        Loggers.warnWithException(LOG, "Native call to getpid() failed", e);
      }
    }
    if (pid == null) {
      try {
        String pidJmx = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        pid = Integer.parseInt(pidJmx);
        LOG.info("PID obtained through JMX: {}", pid);
      } catch (Exception e) {
        Loggers.warnWithException(LOG, "Failed to obtain PID from JMX", e);
      }
    }
    if (pid == null) {
      pid = new Random().nextInt();
      LOG.warn("Could not determine PID, falling back to a random integer: {}", pid);
    }
    ClassLoader loader = Uuids.class.getClassLoader();
    int loaderId = loader != null ? System.identityHashCode(loader) : 0;
    return Integer.toHexString(pid) + Integer.toHexString(loaderId);
  }

  private static void update(MessageDigest digest, String value) {
    if (value != null) {
      digest.update(value.getBytes(Charsets.UTF_8));
    }
  }

  private static long makeClockSeqAndNode() {
    long clock = new Random(System.currentTimeMillis()).nextLong();
    long node = makeNode();

    long lsb = 0;
    lsb |= (clock & 0x0000000000003FFFL) << 48;
    lsb |= 0x8000000000000000L;
    lsb |= node;
    return lsb;
  }

  /**
   * Creates a new random (version 4) UUID.
   *
   * <p>This method is just a convenience for {@code UUID.randomUUID()}.
   */
  public static UUID random() {
    return UUID.randomUUID();
  }

  /**
   * Creates a new time-based (version 1) UUID.
   *
   * <p>UUIDs generated by this method are suitable for use with the {@code timeuuid} Cassandra
   * type. In particular the generated UUID includes the timestamp of its generation.
   *
   * <p>Note that there is no way to provide your own timestamp. This is deliberate, as we feel that
   * this does not conform to the UUID specification, and therefore don't want to encourage it
   * through the API. If you want to do it anyway, use the following workaround:
   *
   * <pre>
   * Random random = new Random();
   * UUID uuid = new UUID(UUIDs.startOf(userProvidedTimestamp).getMostSignificantBits(), random.nextLong());
   * </pre>
   *
   * If you simply need to perform a range query on a {@code timeuuid} column, use the "fake" UUID
   * generated by {@link #startOf(long)} and {@link #endOf(long)}.
   */
  public static UUID timeBased() {
    return new UUID(makeMsb(getCurrentTimestamp()), CLOCK_SEQ_AND_NODE);
  }

  /**
   * Creates a "fake" time-based UUID that sorts as the smallest possible version 1 UUID generated
   * at the provided timestamp.
   *
   * <p>Such created UUIDs are useful in queries to select a time range of a {@code timeuuid}
   * column.
   *
   * <p>The UUIDs created by this method <b>are not unique</b> and as such are <b>not</b> suitable
   * for anything else than querying a specific time range. In particular, you should not insert
   * such UUIDs. "True" UUIDs from user-provided timestamps are not supported (see {@link
   * #timeBased()} for more explanations).
   *
   * <p>Also, the timestamp to provide as a parameter must be a Unix timestamp (as returned by
   * {@link System#currentTimeMillis} or {@link Date#getTime}), and <em>not</em> a count of
   * 100-nanosecond intervals since 00:00:00.00, 15 October 1582 (as required by RFC-4122).
   *
   * <p>In other words, given a UUID {@code uuid}, you should never call {@code
   * startOf(uuid.timestamp())} but rather {@code startOf(unixTimestamp(uuid))}.
   *
   * <p>Lastly, please note that Cassandra's {@code timeuuid} sorting is not compatible with {@link
   * UUID#compareTo} and hence the UUIDs created by this method are not necessarily lower bound for
   * that latter method.
   *
   * @param timestamp the Unix timestamp for which the created UUID must be a lower bound.
   * @return the smallest (for Cassandra {@code timeuuid} sorting) UUID of {@code timestamp}.
   */
  public static UUID startOf(long timestamp) {
    return new UUID(makeMsb(fromUnixTimestamp(timestamp)), MIN_CLOCK_SEQ_AND_NODE);
  }

  /**
   * Creates a "fake" time-based UUID that sorts as the biggest possible version 1 UUID generated at
   * the provided timestamp.
   *
   * <p>See {@link #startOf(long)} for explanations about the intended usage of such UUID.
   *
   * @param timestamp the Unix timestamp for which the created UUID must be an upper bound.
   * @return the biggest (for Cassandra {@code timeuuid} sorting) UUID of {@code timestamp}.
   */
  public static UUID endOf(long timestamp) {
    long uuidTstamp = fromUnixTimestamp(timestamp + 1) - 1;
    return new UUID(makeMsb(uuidTstamp), MAX_CLOCK_SEQ_AND_NODE);
  }

  /**
   * Returns the Unix timestamp contained by the provided time-based UUID.
   *
   * <p>This method is not equivalent to {@link UUID#timestamp()}. More precisely, a version 1 UUID
   * stores a timestamp that represents the number of 100-nanoseconds intervals since midnight, 15
   * October 1582 and that is what {@link UUID#timestamp()} returns. This method however converts
   * that timestamp to the equivalent Unix timestamp in milliseconds, i.e. a timestamp representing
   * a number of milliseconds since midnight, January 1, 1970 UTC. In particular, the timestamps
   * returned by this method are comparable to the timestamps returned by {@link
   * System#currentTimeMillis}, {@link Date#getTime}, etc.
   *
   * @throws IllegalArgumentException if {@code uuid} is not a version 1 UUID.
   */
  public static long unixTimestamp(UUID uuid) {
    if (uuid.version() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "Can only retrieve the unix timestamp for version 1 uuid (provided version %d)",
              uuid.version()));
    }
    long timestamp = uuid.timestamp();
    return (timestamp / 10000) + START_EPOCH;
  }

  // Use {@link System#currentTimeMillis} for a base time in milliseconds, and if we are in the same
  // millisecond as the previous generation, increment the number of nanoseconds.
  // However, since the precision is 100-nanosecond intervals, we can only generate 10K UUIDs within
  // a millisecond safely. If we detect we have already generated that much UUIDs within a
  // millisecond (which, while admittedly unlikely in a real application, is very achievable on even
  // modest machines), then we stall the generator (busy spin) until the next millisecond as
  // required by the RFC.
  private static long getCurrentTimestamp() {
    while (true) {
      long now = fromUnixTimestamp(System.currentTimeMillis());
      long last = lastTimestamp.get();
      if (now > last) {
        if (lastTimestamp.compareAndSet(last, now)) {
          return now;
        }
      } else {
        long lastMillis = millisOf(last);
        // If the clock went back in time, bail out
        if (millisOf(now) < millisOf(last)) {
          return lastTimestamp.incrementAndGet();
        }
        long candidate = last + 1;
        // If we've generated more than 10k uuid in that millisecond, restart the whole process
        // until we get to the next millis. Otherwise, we try use our candidate ... unless we've
        // been beaten by another thread in which case we try again.
        if (millisOf(candidate) == lastMillis && lastTimestamp.compareAndSet(last, candidate)) {
          return candidate;
        }
      }
    }
  }

  @VisibleForTesting
  static long fromUnixTimestamp(long tstamp) {
    return (tstamp - START_EPOCH) * 10000;
  }

  private static long millisOf(long timestamp) {
    return timestamp / 10000;
  }

  @VisibleForTesting
  static long makeMsb(long timestamp) {
    long msb = 0L;
    msb |= (0x00000000ffffffffL & timestamp) << 32;
    msb |= (0x0000ffff00000000L & timestamp) >>> 16;
    msb |= (0x0fff000000000000L & timestamp) >>> 48;
    msb |= 0x0000000000001000L; // sets the version to 1.
    return msb;
  }

  private static Set<String> getAllLocalAddresses() {
    Set<String> allIps = new HashSet<>();
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      allIps.add(localhost.toString());
      // Also return the hostname if available, it won't hurt (this does a dns lookup, it's only
      // done once at startup)
      allIps.add(localhost.getCanonicalHostName());
      InetAddress[] allMyIps = InetAddress.getAllByName(localhost.getCanonicalHostName());
      if (allMyIps != null) {
        for (InetAddress allMyIp : allMyIps) {
          allIps.add(allMyIp.toString());
        }
      }
    } catch (UnknownHostException e) {
      // Ignore, we'll try the network interfaces anyway
    }

    try {
      Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
      if (en != null) {
        while (en.hasMoreElements()) {
          Enumeration<InetAddress> enumIpAddr = en.nextElement().getInetAddresses();
          while (enumIpAddr.hasMoreElements()) {
            allIps.add(enumIpAddr.nextElement().toString());
          }
        }
      }
    } catch (SocketException e) {
      // Ignore, if we've really got nothing so far, we'll throw an exception
    }
    return allIps;
  }
}
