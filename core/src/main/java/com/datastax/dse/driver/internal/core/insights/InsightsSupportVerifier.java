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
