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
package com.datastax.oss.driver.api.core.metadata.diagnostic;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Map;

/**
 * A health diagnostic, containing a health {@link Status} and, optionally, some free-form details
 * about the diagnostic.
 */
public interface Diagnostic {

  /** @return the {@link Status} of this diagnostic. */
  @NonNull
  Status getStatus();

  /**
   * @return details about this diagnostic, in a generic form that can be easily consumed by
   *     monitoring tools.
   */
  @NonNull
  default Map<String, Object> getDetails() {
    return Collections.emptyMap();
  }
}
