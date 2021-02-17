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

import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class PeerRowValidator {

  /** Returns {@code true} if the given peer row is valid, and {@code false} otherwise. */
  public static boolean isValid(@NonNull AdminRow peerRow) {

    boolean hasPeersRpcAddress = !peerRow.isNull("rpc_address");
    boolean hasPeersV2RpcAddress =
        !peerRow.isNull("native_address") && !peerRow.isNull("native_port");
    boolean hasRpcAddress = hasPeersRpcAddress || hasPeersV2RpcAddress;

    return hasRpcAddress
        && !peerRow.isNull("host_id")
        && !peerRow.isNull("data_center")
        && !peerRow.isNull("rack")
        && !peerRow.isNull("tokens")
        && !peerRow.isNull("schema_version");
  }
}
