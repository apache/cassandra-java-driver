package com.datastax.oss.driver.internal.core.loadbalancing.helper;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LocalRackHelper} that fetches the local rack from the driver
 * configuration. If no user-supplied rack can be retrieved, it returns {@link Optional#empty
 * empty}.
 */
@ThreadSafe
public class OptionalLocalRackHelper implements LocalRackHelper {
  private static final Logger LOG = LoggerFactory.getLogger(OptionalLocalRackHelper.class);

  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  public OptionalLocalRackHelper(
      @NonNull DriverExecutionProfile profile, @NonNull String logPrefix) {
    this.profile = profile;
    this.logPrefix = logPrefix;
  }

  @NonNull
  @Override
  public Optional<String> discoverLocalRack(@NonNull Map<UUID, Node> nodes) {
    if (profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK)) {
      String localRack = profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK);
      LOG.debug("[{}] Local rack set from configuration: {}", logPrefix, localRack);
      return Optional.of(localRack);
    } else {
      LOG.debug("[{}] Local rack not set, rack awareness will be disabled", logPrefix);
      return Optional.empty();
    }
  }
}
