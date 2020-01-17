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
package com.datastax.dse.driver.internal.core.loadbalancing;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * @deprecated This class only exists for backward compatibility. It is equivalent to {@link
 *     DcInferringLoadBalancingPolicy}, which should now be used instead.
 */
@Deprecated
public class DseDcInferringLoadBalancingPolicy extends DcInferringLoadBalancingPolicy {
  public DseDcInferringLoadBalancingPolicy(
      @NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
  }
}
