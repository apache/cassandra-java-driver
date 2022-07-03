package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class NodeListRefreshingNodeStateListener extends NodeStateListenerBase {
    private static final Logger LOG = LoggerFactory.getLogger(NodeListRefreshingNodeStateListener.class);

    private final DriverContext context;
    private volatile boolean initialized;
    private volatile boolean refreshInProgress;

    /**
     * Note that this is constructed via reflection in
     * {@link com.datastax.oss.driver.internal.core.context.DefaultDriverContext}
     */
    public NodeListRefreshingNodeStateListener(DriverContext context) {
        this.context = castOrThrow(context);
    }

    @Override
    public void onSessionReady(@NonNull Session session) {
        initialized = true;
    }

    @Override
    public void onAdd(@NonNull Node node) {
        /* Note that this method is always called on the same admin executor thread, so we cannot
         * have race conditions here. */
        try {
            if (initialized && !refreshInProgress && context instanceof InternalDriverContext) {
                InternalDriverContext internalContext = (InternalDriverContext)  context;
                internalContext.getMetadataManager().getMetadata().getNodes().values().stream()
                        .filter(n -> n.getState() == NodeState.DOWN)
                        .findFirst()
                        .ifPresent(downNode -> {
                            LOG.debug("{} was added but {} is DOWN, refreshing node list", node, downNode);
                            refreshInProgress = true;
                            internalContext.getMetadataManager().refreshNodes().thenAccept(v -> refreshInProgress = false);
                        });
            }
        } catch (Throwable t) {
            LOG.warn("Failure refreshing node list on addition of {}", node, t);
        }
    }
}
