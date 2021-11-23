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
package com.datastax.oss.driver.api.core.config;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

/**
 * An in-memory repository of config options, for use with {@link
 * DriverConfigLoader#fromMap(OptionsMap)}.
 *
 * <p>This class is intended for clients who wish to assemble the driver configuration in memory,
 * instead of loading it from configuration files. Note that {@link #driverDefaults()} can be used
 * to pre-initialize the map with the driver's built-in defaults.
 *
 * <p>It functions like a two-dimensional map indexed by execution profile and option. All methods
 * have a profile-less variant that applies to the default profile, for example {@link #get(String,
 * TypedDriverOption)} and {@link #get(TypedDriverOption)}. Options are represented by {@link
 * TypedDriverOption}, which allows this class to enforce additional type-safety guarantees (an
 * option can only be set to a value of its intended type).
 *
 * <p>This class is mutable and thread-safe. Live changes are reflected in real time to the driver
 * session(s) that use this configuration.
 *
 * @since 4.6.0
 */
@ThreadSafe
public class OptionsMap implements Serializable {

  private static final long serialVersionUID = 1;

  /**
   * Creates a new instance that contains the driver's default configuration.
   *
   * <p>This will produce a configuration that is equivalent to the {@code reference.conf} file
   * bundled with the driver (however, this method does not load any file, and doesn't require
   * Typesafe config in the classpath).
   */
  @NonNull
  public static OptionsMap driverDefaults() {
    OptionsMap source = new OptionsMap();
    fillWithDriverDefaults(source);
    return source;
  }

  private final ConcurrentHashMap<String, Map<DriverOption, Object>> map;

  private final List<Consumer<OptionsMap>> changeListeners = new CopyOnWriteArrayList<>();

  public OptionsMap() {
    this(new ConcurrentHashMap<>());
  }

  private OptionsMap(ConcurrentHashMap<String, Map<DriverOption, Object>> map) {
    this.map = map;
  }

  /**
   * Associates the specified value for the specified option, in the specified execution profile.
   *
   * @return the previous value associated with {@code option}, or {@code null} if the option was
   *     not defined.
   */
  @Nullable
  public <ValueT> ValueT put(
      @NonNull String profile, @NonNull TypedDriverOption<ValueT> option, @NonNull ValueT value) {
    Objects.requireNonNull(option, "option");
    Objects.requireNonNull(value, "value");
    Object previous = getProfileMap(profile).put(option.getRawOption(), value);
    if (!value.equals(previous)) {
      for (Consumer<OptionsMap> listener : changeListeners) {
        listener.accept(this);
      }
    }
    return cast(previous);
  }

  /**
   * Associates the specified value for the specified option, in the default execution profile.
   *
   * @return the previous value associated with {@code option}, or {@code null} if the option was
   *     not defined.
   */
  @Nullable
  public <ValueT> ValueT put(@NonNull TypedDriverOption<ValueT> option, @NonNull ValueT value) {
    return put(DriverExecutionProfile.DEFAULT_NAME, option, value);
  }

  /**
   * Returns the value to which the specified option is mapped in the specified profile, or {@code
   * null} if the option is not defined.
   */
  @Nullable
  public <ValueT> ValueT get(@NonNull String profile, @NonNull TypedDriverOption<ValueT> option) {
    Objects.requireNonNull(option, "option");
    Object result = getProfileMap(profile).get(option.getRawOption());
    return cast(result);
  }

  /**
   * Returns the value to which the specified option is mapped in the default profile, or {@code
   * null} if the option is not defined.
   */
  @Nullable
  public <ValueT> ValueT get(@NonNull TypedDriverOption<ValueT> option) {
    return get(DriverExecutionProfile.DEFAULT_NAME, option);
  }

  /**
   * Removes the specified option from the specified profile.
   *
   * @return the previous value associated with {@code option}, or {@code null} if the option was
   *     not defined.
   */
  @Nullable
  public <ValueT> ValueT remove(
      @NonNull String profile, @NonNull TypedDriverOption<ValueT> option) {
    Objects.requireNonNull(option, "option");
    Object previous = getProfileMap(profile).remove(option.getRawOption());
    if (previous != null) {
      for (Consumer<OptionsMap> listener : changeListeners) {
        listener.accept(this);
      }
    }
    return cast(previous);
  }

  /**
   * Removes the specified option from the default profile.
   *
   * @return the previous value associated with {@code option}, or {@code null} if the option was
   *     not defined.
   */
  @Nullable
  public <ValueT> ValueT remove(@NonNull TypedDriverOption<ValueT> option) {
    return remove(DriverExecutionProfile.DEFAULT_NAME, option);
  }

  /**
   * Registers a listener that will get notified when this object changes.
   *
   * <p>This is mostly for internal use by the driver. Note that listeners are transient, and not
   * taken into account by {@link #equals(Object)} and {@link #hashCode()}.
   */
  public void addChangeListener(@NonNull Consumer<OptionsMap> listener) {
    changeListeners.add(Objects.requireNonNull(listener));
  }

  /**
   * Unregisters a listener that was previously registered with {@link
   * #addChangeListener(Consumer)}.
   *
   * @return {@code true} if the listener was indeed registered for this object.
   */
  public boolean removeChangeListener(@NonNull Consumer<OptionsMap> listener) {
    return changeListeners.remove(Objects.requireNonNull(listener));
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof OptionsMap) {
      OptionsMap that = (OptionsMap) other;
      return this.map.equals(that.map);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  /**
   * Returns a live view of this object, using the driver's untyped {@link DriverOption}.
   *
   * <p>This is intended for internal usage by the driver. Modifying the resulting map is strongly
   * discouraged, as it could break the type-safety guarantees provided by the public methods.
   */
  @NonNull
  protected Map<String, Map<DriverOption, Object>> asRawMap() {
    return map;
  }

  @NonNull
  private Map<DriverOption, Object> getProfileMap(@NonNull String profile) {
    Objects.requireNonNull(profile, "profile");
    return map.computeIfAbsent(profile, p -> new ConcurrentHashMap<>());
  }

  // Isolate the suppressed warning for retrieval. The cast should always succeed unless the user
  // messes with asMap() directly.
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  @Nullable
  private <ValueT> ValueT cast(@Nullable Object value) {
    return (ValueT) value;
  }

  /**
   * This object gets replaced by an internal proxy for serialization.
   *
   * @serialData the serialized form of the {@code Map<String, Map<DriverOption, Object>>} used to
   *     store options internally (listeners are transient).
   */
  private Object writeReplace() {
    return new SerializationProxy(this.map);
  }

  // Should never be called since we serialize a proxy
  @SuppressWarnings("UnusedVariable")
  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("Proxy required");
  }

  protected static void fillWithDriverDefaults(OptionsMap map) {
    Duration initQueryTimeout = Duration.ofSeconds(5);
    Duration requestTimeout = Duration.ofSeconds(2);
    int requestPageSize = 5000;
    int continuousMaxPages = 0;
    int continuousMaxPagesPerSecond = 0;
    int continuousMaxEnqueuedPages = 4;

    // Sorted by order of appearance in reference.conf:

    // Skip CONFIG_RELOAD_INTERVAL because the map-based config doesn't need periodic reloading
    map.put(TypedDriverOption.REQUEST_TIMEOUT, requestTimeout);
    map.put(TypedDriverOption.REQUEST_CONSISTENCY, "LOCAL_ONE");
    map.put(TypedDriverOption.REQUEST_PAGE_SIZE, requestPageSize);
    map.put(TypedDriverOption.REQUEST_SERIAL_CONSISTENCY, "SERIAL");
    map.put(TypedDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, false);
    map.put(TypedDriverOption.GRAPH_TRAVERSAL_SOURCE, "g");
    map.put(TypedDriverOption.LOAD_BALANCING_POLICY_CLASS, "DefaultLoadBalancingPolicy");
    map.put(TypedDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true);
    map.put(TypedDriverOption.SESSION_LEAK_THRESHOLD, 4);
    map.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(5));
    map.put(TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, initQueryTimeout);
    map.put(TypedDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, initQueryTimeout);
    map.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1);
    map.put(TypedDriverOption.CONNECTION_POOL_REMOTE_SIZE, 1);
    map.put(TypedDriverOption.CONNECTION_MAX_REQUESTS, 1024);
    map.put(TypedDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, 256);
    map.put(TypedDriverOption.CONNECTION_WARN_INIT_ERROR, true);
    map.put(TypedDriverOption.RECONNECT_ON_INIT, false);
    map.put(TypedDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy");
    map.put(TypedDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1));
    map.put(TypedDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(60));
    map.put(TypedDriverOption.RETRY_POLICY_CLASS, "DefaultRetryPolicy");
    map.put(TypedDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS, "NoSpeculativeExecutionPolicy");
    map.put(TypedDriverOption.TIMESTAMP_GENERATOR_CLASS, "AtomicTimestampGenerator");
    map.put(TypedDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD, Duration.ofSeconds(1));
    map.put(TypedDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL, Duration.ofSeconds(10));
    map.put(TypedDriverOption.TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK, false);
    map.put(TypedDriverOption.REQUEST_THROTTLER_CLASS, "PassThroughRequestThrottler");
    map.put(TypedDriverOption.ADDRESS_TRANSLATOR_CLASS, "PassThroughAddressTranslator");
    map.put(TypedDriverOption.RESOLVE_CONTACT_POINTS, true);
    map.put(TypedDriverOption.PROTOCOL_MAX_FRAME_LENGTH, 256L * 1024 * 1024);
    map.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, true);
    map.put(TypedDriverOption.REQUEST_TRACE_ATTEMPTS, 5);
    map.put(TypedDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(3));
    map.put(TypedDriverOption.REQUEST_TRACE_CONSISTENCY, "ONE");
    map.put(TypedDriverOption.REQUEST_LOG_WARNINGS, true);
    map.put(TypedDriverOption.GRAPH_PAGING_ENABLED, "AUTO");
    map.put(TypedDriverOption.GRAPH_CONTINUOUS_PAGING_PAGE_SIZE, requestPageSize);
    map.put(TypedDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES, continuousMaxPages);
    map.put(
        TypedDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND,
        continuousMaxPagesPerSecond);
    map.put(
        TypedDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, continuousMaxEnqueuedPages);
    map.put(TypedDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, requestPageSize);
    map.put(TypedDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES, false);
    map.put(TypedDriverOption.CONTINUOUS_PAGING_MAX_PAGES, continuousMaxPages);
    map.put(TypedDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, continuousMaxPagesPerSecond);
    map.put(TypedDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, continuousMaxEnqueuedPages);
    map.put(TypedDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofSeconds(2));
    map.put(TypedDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofSeconds(1));
    map.put(TypedDriverOption.MONITOR_REPORTING_ENABLED, true);
    map.put(TypedDriverOption.METRICS_SESSION_ENABLED, Collections.emptyList());
    map.put(TypedDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST, Duration.ofSeconds(3));
    map.put(TypedDriverOption.METRICS_SESSION_CQL_REQUESTS_LOWEST, Duration.ofMillis(1));
    map.put(TypedDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS, 3);
    map.put(TypedDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL, Duration.ofMinutes(5));
    map.put(TypedDriverOption.METRICS_SESSION_THROTTLING_HIGHEST, Duration.ofSeconds(3));
    map.put(TypedDriverOption.METRICS_SESSION_THROTTLING_LOWEST, Duration.ofMillis(1));
    map.put(TypedDriverOption.METRICS_SESSION_THROTTLING_DIGITS, 3);
    map.put(TypedDriverOption.METRICS_SESSION_THROTTLING_INTERVAL, Duration.ofMinutes(5));
    map.put(
        TypedDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        Duration.ofMinutes(2));
    map.put(
        TypedDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST,
        Duration.ofMillis(10));
    map.put(TypedDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS, 3);
    map.put(
        TypedDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL,
        Duration.ofMinutes(5));
    map.put(TypedDriverOption.METRICS_FACTORY_CLASS, "DefaultMetricsFactory");
    map.put(TypedDriverOption.METRICS_ID_GENERATOR_CLASS, "DefaultMetricIdGenerator");
    map.put(TypedDriverOption.METRICS_SESSION_GRAPH_REQUESTS_HIGHEST, Duration.ofSeconds(12));
    map.put(TypedDriverOption.METRICS_SESSION_GRAPH_REQUESTS_LOWEST, Duration.ofMillis(1));
    map.put(TypedDriverOption.METRICS_SESSION_GRAPH_REQUESTS_DIGITS, 3);
    map.put(TypedDriverOption.METRICS_SESSION_GRAPH_REQUESTS_INTERVAL, Duration.ofMinutes(5));
    map.put(TypedDriverOption.METRICS_NODE_ENABLED, Collections.emptyList());
    map.put(TypedDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST, Duration.ofSeconds(3));
    map.put(TypedDriverOption.METRICS_NODE_CQL_MESSAGES_LOWEST, Duration.ofMillis(1));
    map.put(TypedDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS, 3);
    map.put(TypedDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL, Duration.ofMinutes(5));
    map.put(TypedDriverOption.METRICS_NODE_GRAPH_MESSAGES_HIGHEST, Duration.ofSeconds(3));
    map.put(TypedDriverOption.METRICS_NODE_GRAPH_MESSAGES_LOWEST, Duration.ofMillis(1));
    map.put(TypedDriverOption.METRICS_NODE_GRAPH_MESSAGES_DIGITS, 3);
    map.put(TypedDriverOption.METRICS_NODE_GRAPH_MESSAGES_INTERVAL, Duration.ofMinutes(5));
    map.put(TypedDriverOption.METRICS_NODE_EXPIRE_AFTER, Duration.ofHours(1));
    map.put(TypedDriverOption.SOCKET_TCP_NODELAY, true);
    map.put(TypedDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(30));
    map.put(TypedDriverOption.HEARTBEAT_TIMEOUT, initQueryTimeout);
    map.put(TypedDriverOption.METADATA_TOPOLOGY_WINDOW, Duration.ofSeconds(1));
    map.put(TypedDriverOption.METADATA_TOPOLOGY_MAX_EVENTS, 20);
    map.put(TypedDriverOption.METADATA_SCHEMA_ENABLED, true);
    map.put(
        TypedDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
        ImmutableList.of("!system", "!/^system_.*/", "!/^dse_.*/", "!solr_admin", "!OpsCenter"));
    map.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, requestTimeout);
    map.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE, requestPageSize);
    map.put(TypedDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofSeconds(1));
    map.put(TypedDriverOption.METADATA_SCHEMA_MAX_EVENTS, 20);
    map.put(TypedDriverOption.METADATA_TOKEN_MAP_ENABLED, true);
    map.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, initQueryTimeout);
    map.put(TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL, Duration.ofMillis(200));
    map.put(TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(10));
    map.put(TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN, true);
    map.put(TypedDriverOption.PREPARE_ON_ALL_NODES, true);
    map.put(TypedDriverOption.REPREPARE_ENABLED, true);
    map.put(TypedDriverOption.REPREPARE_CHECK_SYSTEM_TABLE, false);
    map.put(TypedDriverOption.REPREPARE_MAX_STATEMENTS, 0);
    map.put(TypedDriverOption.REPREPARE_MAX_PARALLELISM, 100);
    map.put(TypedDriverOption.REPREPARE_TIMEOUT, initQueryTimeout);
    map.put(TypedDriverOption.NETTY_DAEMON, false);
    map.put(TypedDriverOption.NETTY_IO_SIZE, 0);
    map.put(TypedDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, 2);
    map.put(TypedDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT, 15);
    map.put(TypedDriverOption.NETTY_IO_SHUTDOWN_UNIT, "SECONDS");
    map.put(TypedDriverOption.NETTY_ADMIN_SIZE, 2);
    map.put(TypedDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 2);
    map.put(TypedDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT, 15);
    map.put(TypedDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT, "SECONDS");
    map.put(TypedDriverOption.NETTY_TIMER_TICK_DURATION, Duration.ofMillis(100));
    map.put(TypedDriverOption.NETTY_TIMER_TICKS_PER_WHEEL, 2048);
    map.put(TypedDriverOption.COALESCER_INTERVAL, Duration.of(10, ChronoUnit.MICROS));
    map.put(TypedDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC, 0);
    map.put(TypedDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS, false);
  }

  @Immutable
  private static class SerializationProxy implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConcurrentHashMap<String, Map<DriverOption, Object>> map;

    private SerializationProxy(ConcurrentHashMap<String, Map<DriverOption, Object>> map) {
      this.map = map;
    }

    private Object readResolve() {
      return new OptionsMap(map);
    }
  }
}
