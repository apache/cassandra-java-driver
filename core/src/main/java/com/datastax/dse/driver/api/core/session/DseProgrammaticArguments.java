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
package com.datastax.dse.driver.api.core.session;

import com.datastax.oss.driver.api.core.CqlSession;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.UUID;

/**
 * @deprecated All DSE functionality is now available directly on {@link CqlSession}. This type is
 *     preserved for backward compatibility, but clients should now build {@link CqlSession}
 *     instances instead of DSE sessions.
 */
@Deprecated
public class DseProgrammaticArguments {

  @NonNull
  public static Builder builder() {
    return new Builder();
  }

  private final UUID startupClientId;
  private final String startupApplicationName;
  private final String startupApplicationVersion;

  private DseProgrammaticArguments(
      @Nullable UUID startupClientId,
      @Nullable String startupApplicationName,
      @Nullable String startupApplicationVersion) {
    this.startupClientId = startupClientId;
    this.startupApplicationName = startupApplicationName;
    this.startupApplicationVersion = startupApplicationVersion;
  }

  @Nullable
  public UUID getStartupClientId() {
    return startupClientId;
  }

  @Nullable
  public String getStartupApplicationName() {
    return startupApplicationName;
  }

  @Nullable
  public String getStartupApplicationVersion() {
    return startupApplicationVersion;
  }

  public static class Builder {

    private UUID startupClientId;
    private String startupApplicationName;
    private String startupApplicationVersion;

    @NonNull
    public Builder withStartupClientId(@Nullable UUID startupClientId) {
      this.startupClientId = startupClientId;
      return this;
    }

    @NonNull
    public Builder withStartupApplicationName(@Nullable String startupApplicationName) {
      this.startupApplicationName = startupApplicationName;
      return this;
    }

    @NonNull
    public Builder withStartupApplicationVersion(@Nullable String startupApplicationVersion) {
      this.startupApplicationVersion = startupApplicationVersion;
      return this;
    }

    @NonNull
    public DseProgrammaticArguments build() {
      return new DseProgrammaticArguments(
          startupClientId, startupApplicationName, startupApplicationVersion);
    }
  }
}
