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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Debouncer;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the immutable instance of the {@link Metadata}, and handles requests to update it. */
@ThreadSafe
public class MetadataManager implements AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

  static final EndPoint DEFAULT_CONTACT_POINT =
      new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));

  private final InternalDriverContext context;
  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final DriverExecutionProfile config;
  private final SingleThreaded singleThreaded;
  private final ControlConnection controlConnection;

  private volatile DefaultMetadata metadata; // only updated from adminExecutor
  private volatile boolean schemaEnabledInConfig;
  private volatile List<String> refreshedKeyspaces;
  private volatile Boolean schemaEnabledProgrammatically;
  private volatile boolean tokenMapEnabled;
  private volatile Set<DefaultNode> contactPoints;
  private volatile boolean wasImplicitContactPoint;

  public MetadataManager(InternalDriverContext context) {
    this(context, DefaultMetadata.EMPTY);
  }

  protected MetadataManager(InternalDriverContext context, DefaultMetadata initialMetadata) {
    this.context = context;
    this.metadata = initialMetadata;
    this.logPrefix = context.getSessionName();
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.config = context.getConfig().getDefaultProfile();
    this.singleThreaded = new SingleThreaded(context, config);
    this.controlConnection = context.getControlConnection();
    this.schemaEnabledInConfig = config.getBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED);
    this.refreshedKeyspaces =
        config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList());
    this.tokenMapEnabled = config.getBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED);

    context.getEventBus().register(ConfigChangeEvent.class, this::onConfigChanged);
  }

  private void onConfigChanged(@SuppressWarnings("unused") ConfigChangeEvent event) {
    boolean schemaEnabledBefore = isSchemaEnabled();
    boolean tokenMapEnabledBefore = tokenMapEnabled;
    List<String> keyspacesBefore = this.refreshedKeyspaces;

    this.schemaEnabledInConfig = config.getBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED);
    this.refreshedKeyspaces =
        config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList());
    this.tokenMapEnabled = config.getBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED);

    if ((!schemaEnabledBefore
            || !keyspacesBefore.equals(refreshedKeyspaces)
            || (!tokenMapEnabledBefore && tokenMapEnabled))
        && isSchemaEnabled()) {
      refreshSchema(null, false, true);
    }
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public void addContactPoints(Set<EndPoint> providedContactPoints) {
    // Convert the EndPoints to Nodes, but we can't put them into the Metadata yet, because we
    // don't know their host_id. So store them in a volatile field instead, they will get copied
    // during the first node refresh.
    ImmutableSet.Builder<DefaultNode> contactPointsBuilder = ImmutableSet.builder();
    if (providedContactPoints == null || providedContactPoints.isEmpty()) {
      LOG.info(
          "[{}] No contact points provided, defaulting to {}", logPrefix, DEFAULT_CONTACT_POINT);
      this.wasImplicitContactPoint = true;
      contactPointsBuilder.add(new DefaultNode(DEFAULT_CONTACT_POINT, context));
    } else {
      for (EndPoint endPoint : providedContactPoints) {
        contactPointsBuilder.add(new DefaultNode(endPoint, context));
      }
    }
    this.contactPoints = contactPointsBuilder.build();
    LOG.debug("[{}] Adding initial contact points {}", logPrefix, contactPoints);
  }

  /**
   * The contact points that were used by the driver to initialize. If none were provided
   * explicitly, this will be the default (127.0.0.1:9042).
   *
   * @see #wasImplicitContactPoint()
   */
  public Set<DefaultNode> getContactPoints() {
    return contactPoints;
  }

  /** Whether the default contact point was used (because none were provided explicitly). */
  public boolean wasImplicitContactPoint() {
    return wasImplicitContactPoint;
  }

  public CompletionStage<Void> refreshNodes() {
    return context
        .getTopologyMonitor()
        .refreshNodeList()
        .thenApplyAsync(singleThreaded::refreshNodes, adminExecutor);
  }

  public CompletionStage<Void> refreshNode(Node node) {
    return context
        .getTopologyMonitor()
        .refreshNode(node)
        .thenApplyAsync(
            maybeInfo -> {
              if (maybeInfo.isPresent()) {
                boolean tokensChanged =
                    NodesRefresh.copyInfos(maybeInfo.get(), (DefaultNode) node, null, logPrefix);
                if (tokensChanged) {
                  apply(new TokensChangedRefresh());
                }
              } else {
                LOG.debug(
                    "[{}] Topology monitor did not return any info for the refresh of {}, skipping",
                    logPrefix,
                    node);
              }
              return null;
            },
            adminExecutor);
  }

  public void addNode(InetSocketAddress broadcastRpcAddress) {
    context
        .getTopologyMonitor()
        .getNewNodeInfo(broadcastRpcAddress)
        .whenCompleteAsync(
            (info, error) -> {
              if (error != null) {
                LOG.debug(
                    "[{}] Error refreshing node info for {}, "
                        + "this will be retried on the next full refresh",
                    logPrefix,
                    broadcastRpcAddress,
                    error);
              } else {
                singleThreaded.addNode(broadcastRpcAddress, info.orElse(null));
              }
            },
            adminExecutor);
  }

  public void removeNode(InetSocketAddress broadcastRpcAddress) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.removeNode(broadcastRpcAddress));
  }

  /**
   * @param keyspace if this refresh was triggered by an event, that event's keyspace, otherwise
   *     null (this is only used to discard the event if it targets a keyspace that we're ignoring)
   * @param evenIfDisabled force the refresh even if schema is currently disabled (used for user
   *     request)
   * @param flushNow bypass the debouncer and force an immediate refresh (used to avoid a delay at
   *     startup)
   */
  public CompletionStage<Metadata> refreshSchema(
      String keyspace, boolean evenIfDisabled, boolean flushNow) {
    CompletableFuture<Metadata> future = new CompletableFuture<>();
    RunOrSchedule.on(
        adminExecutor,
        () -> singleThreaded.refreshSchema(keyspace, evenIfDisabled, flushNow, future));
    return future;
  }

  public boolean isSchemaEnabled() {
    return (schemaEnabledProgrammatically != null)
        ? schemaEnabledProgrammatically
        : schemaEnabledInConfig;
  }

  public CompletionStage<Metadata> setSchemaEnabled(Boolean newValue) {
    boolean wasEnabledBefore = isSchemaEnabled();
    schemaEnabledProgrammatically = newValue;
    if (!wasEnabledBefore && isSchemaEnabled()) {
      return refreshSchema(null, false, true);
    } else {
      return CompletableFuture.completedFuture(metadata);
    }
  }

  /**
   * Returns a future that completes after the first schema refresh attempt, whether that attempt
   * succeeded or not (we wait for that refresh at init, but if it fails it's not fatal).
   */
  public CompletionStage<Void> firstSchemaRefreshFuture() {
    return singleThreaded.firstSchemaRefreshFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return this.closeAsync();
  }

  private class SingleThreaded {
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final CompletableFuture<Void> firstSchemaRefreshFuture = new CompletableFuture<>();
    private final Debouncer<CompletableFuture<Metadata>, CompletableFuture<Metadata>>
        schemaRefreshDebouncer;
    private final SchemaQueriesFactory schemaQueriesFactory;
    private final SchemaParserFactory schemaParserFactory;

    // We don't allow concurrent schema refreshes. If one is already running, the next one is queued
    // (and the ones after that are merged with the queued one).
    private CompletableFuture<Metadata> currentSchemaRefresh;
    private CompletableFuture<Metadata> queuedSchemaRefresh;

    private boolean didFirstNodeListRefresh;

    private SingleThreaded(InternalDriverContext context, DriverExecutionProfile config) {
      this.schemaRefreshDebouncer =
          new Debouncer<>(
              adminExecutor,
              this::coalesceSchemaRequests,
              this::startSchemaRequest,
              config.getDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW),
              config.getInt(DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS));
      this.schemaQueriesFactory = context.getSchemaQueriesFactory();
      this.schemaParserFactory = context.getSchemaParserFactory();
    }

    private Void refreshNodes(Iterable<NodeInfo> nodeInfos) {
      MetadataRefresh refresh =
          didFirstNodeListRefresh
              ? new FullNodeListRefresh(nodeInfos)
              : new InitialNodeListRefresh(nodeInfos, contactPoints);
      didFirstNodeListRefresh = true;
      return apply(refresh);
    }

    private void addNode(InetSocketAddress address, NodeInfo info) {
      try {
        if (info != null) {
          if (!address.equals(info.getBroadcastRpcAddress().orElse(null))) {
            // This would be a bug in the TopologyMonitor, protect against it
            LOG.warn(
                "[{}] Received a request to add a node for broadcast RPC address {}, "
                    + "but the provided info reports {}, ignoring it",
                logPrefix,
                address,
                info.getBroadcastAddress());
          } else {
            apply(new AddNodeRefresh(info));
          }
        } else {
          LOG.debug(
              "[{}] Ignoring node addition for {} because the "
                  + "topology monitor didn't return any information",
              logPrefix,
              address);
        }
      } catch (Throwable t) {
        LOG.warn("[" + logPrefix + "] Unexpected exception while handling added node", logPrefix);
      }
    }

    private void removeNode(InetSocketAddress broadcastRpcAddress) {
      apply(new RemoveNodeRefresh(broadcastRpcAddress));
    }

    private void refreshSchema(
        String keyspace,
        boolean evenIfDisabled,
        boolean flushNow,
        CompletableFuture<Metadata> future) {

      if (!didFirstNodeListRefresh) {
        // This happen if the control connection receives a schema event during init. We can't
        // refresh yet because we don't know the nodes' versions, simply ignore.
        future.complete(metadata);
        return;
      }

      // If this is an event, make sure it's not targeting a keyspace that we're ignoring.
      boolean isRefreshedKeyspace =
          keyspace == null || refreshedKeyspaces.isEmpty() || refreshedKeyspaces.contains(keyspace);

      if (isRefreshedKeyspace && (evenIfDisabled || isSchemaEnabled())) {
        acceptSchemaRequest(future, flushNow);
      } else {
        future.complete(metadata);
        singleThreaded.firstSchemaRefreshFuture.complete(null);
      }
    }

    // An external component has requested a schema refresh, feed it to the debouncer.
    private void acceptSchemaRequest(CompletableFuture<Metadata> future, boolean flushNow) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        future.complete(metadata);
      } else {
        schemaRefreshDebouncer.receive(future);
        if (flushNow) {
          schemaRefreshDebouncer.flushNow();
        }
      }
    }

    // Multiple requests have arrived within the debouncer window, coalesce them.
    private CompletableFuture<Metadata> coalesceSchemaRequests(
        List<CompletableFuture<Metadata>> futures) {
      assert adminExecutor.inEventLoop();
      assert !futures.isEmpty();
      // Keep only one, but ensure that the discarded ones will still be completed when we're done
      CompletableFuture<Metadata> result = null;
      for (CompletableFuture<Metadata> future : futures) {
        if (result == null) {
          result = future;
        } else {
          CompletableFutures.completeFrom(result, future);
        }
      }
      return result;
    }

    // The debouncer has flushed, start the actual work.
    private void startSchemaRequest(CompletableFuture<Metadata> future) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        future.complete(metadata);
        return;
      }
      if (currentSchemaRefresh == null) {
        currentSchemaRefresh = future;
        LOG.debug("[{}] Starting schema refresh", logPrefix);
        maybeInitControlConnection()
            .thenCompose(v -> context.getTopologyMonitor().checkSchemaAgreement())
            // 1. Query system tables
            .thenCompose(b -> schemaQueriesFactory.newInstance(future).execute())
            // 2. Parse the rows into metadata objects, put them in a MetadataRefresh
            // 3. Apply the MetadataRefresh
            .thenApplyAsync(this::parseAndApplySchemaRows, adminExecutor)
            .whenComplete(
                (v, error) -> {
                  if (error != null) {
                    Loggers.warnWithException(
                        LOG,
                        "[{}] Unexpected error while refreshing schema, skipping",
                        logPrefix,
                        error);
                  }
                  singleThreaded.firstSchemaRefreshFuture.complete(null);
                });
      } else if (queuedSchemaRefresh == null) {
        queuedSchemaRefresh = future; // wait for our turn
      } else {
        CompletableFutures.completeFrom(queuedSchemaRefresh, future); // join the queued request
      }
    }

    // The control connection may or may not have been initialized already by TopologyMonitor.
    private CompletionStage<Void> maybeInitControlConnection() {
      if (firstSchemaRefreshFuture.isDone()) {
        // Not the first schema refresh, so we know init was attempted already
        return firstSchemaRefreshFuture;
      } else {
        controlConnection.init(false, true, false);
        // The control connection might fail to connect and reattempt, but for the metadata refresh
        // that led us here we only care about the first attempt (metadata is not vital, so if we
        // can't get it right now it's OK to move on)
        return controlConnection.firstConnectionAttemptFuture();
      }
    }

    private Void parseAndApplySchemaRows(SchemaRows schemaRows) {
      assert adminExecutor.inEventLoop();
      assert schemaRows.refreshFuture() == currentSchemaRefresh;
      try {
        SchemaRefresh schemaRefresh = schemaParserFactory.newInstance(schemaRows).parse();
        long start = System.nanoTime();
        apply(schemaRefresh);
        currentSchemaRefresh.complete(metadata);
        LOG.debug(
            "[{}] Applying schema refresh took {}", logPrefix, NanoTime.formatTimeSince(start));
      } catch (Throwable t) {
        currentSchemaRefresh.completeExceptionally(t);
      }
      currentSchemaRefresh = null;
      if (queuedSchemaRefresh != null) {
        CompletableFuture<Metadata> tmp = this.queuedSchemaRefresh;
        this.queuedSchemaRefresh = null;
        startSchemaRequest(tmp);
      }
      return null;
    }

    private void close() {
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Closing", logPrefix);
      // The current schema refresh should fail when its channel gets closed.
      if (queuedSchemaRefresh != null) {
        queuedSchemaRefresh.completeExceptionally(new IllegalStateException("Cluster is closed"));
      }
      closeFuture.complete(null);
    }
  }

  @VisibleForTesting
  Void apply(MetadataRefresh refresh) {
    assert adminExecutor.inEventLoop();
    MetadataRefresh.Result result = refresh.compute(metadata, tokenMapEnabled, context);
    metadata = result.newMetadata;
    boolean isFirstSchemaRefresh =
        refresh instanceof SchemaRefresh && !singleThreaded.firstSchemaRefreshFuture.isDone();
    if (!singleThreaded.closeWasCalled && !isFirstSchemaRefresh) {
      for (Object event : result.events) {
        context.getEventBus().fire(event);
      }
    }
    return null;
  }
}
