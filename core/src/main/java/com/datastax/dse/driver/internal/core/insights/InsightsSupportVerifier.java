/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Collection;

class InsightsSupportVerifier {
  private static final Version minDse6Version = Version.parse("6.0.5");
  private static final Version minDse51Version = Version.parse("5.1.13");
  private static final Version dse600Version = Version.parse("6.0.0");

  static boolean supportsInsights(Collection<Node> nodes) {
    assert minDse6Version != null;
    assert dse600Version != null;
    assert minDse51Version != null;
    if (nodes.isEmpty()) return false;

    for (Node node : nodes) {
      Object version = node.getExtras().get(DseNodeProperties.DSE_VERSION);
      if (version == null) {
        return false;
      }
      Version dseVersion = (Version) version;
      if (!(dseVersion.compareTo(minDse6Version) >= 0
          || (dseVersion.compareTo(dse600Version) < 0
              && dseVersion.compareTo(minDse51Version) >= 0))) {
        return false;
      }
    }
    return true;
  }
}
