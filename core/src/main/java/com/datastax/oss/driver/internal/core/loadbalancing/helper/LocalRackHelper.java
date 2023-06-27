package com.datastax.oss.driver.internal.core.loadbalancing.helper;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface LocalRackHelper {
  @NonNull
  Optional<String> discoverLocalRack(@NonNull Map<UUID, Node> nodes);
}
