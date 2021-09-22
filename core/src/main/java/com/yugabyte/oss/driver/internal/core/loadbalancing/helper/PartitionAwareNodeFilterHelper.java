package com.yugabyte.oss.driver.internal.core.loadbalancing.helper;

import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.DefaultNodeFilterHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.NodeFilterHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link NodeFilterHelper} implementation that fetches the user-supplied filter, if any, from the
 * programmatic configuration API, or else, from the driver configuration. If no user-supplied
 * filter can be retrieved, a dummy filter will be used which accepts all nodes unconditionally.
 *
 * <p>Note that, regardless of the filter supplied by the end user, if a local datacenter is defined
 * the filter returned by this implementation will always reject nodes that report a datacenter
 * different from the local one.
 */
@ThreadSafe
public class PartitionAwareNodeFilterHelper extends DefaultNodeFilterHelper {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareNodeFilterHelper.class);

	public PartitionAwareNodeFilterHelper(
			@NonNull InternalDriverContext context,
			@NonNull DriverExecutionProfile profile,
			@NonNull String logPrefix) {
		super(context, profile, logPrefix);
	}

	@NonNull
	@Override
	public Predicate<Node> createNodeFilter(
			@Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
		return node -> {
			if (!nodeFilterFromConfig().test(node)) {
				LOG.debug(
						"[{}] Ignoring {} because it doesn't match the user-provided predicate",
						logPrefix,
						node);
				return false;
			}
			else if (localDc != null && !localDc.equals(node.getDatacenter())
					&& !isYBStrong()) {
				LOG.debug(
						"[{}] Ignoring {} because it doesn't belong to the local DC {}",
						logPrefix,
						node,
						localDc);
				return false;
			}
			else {
				return true;
			}
		};
	}

	private boolean isYBStrong() {
		String level = this.context.getConfig().getProfile(this.profile.getName())
				.getString(DefaultDriverOption.REQUEST_CONSISTENCY);
		return DefaultConsistencyLevel.QUORUM.name().equalsIgnoreCase(level) || DefaultConsistencyLevel.LOCAL_ONE.name()
				.equalsIgnoreCase(level);

	}
}