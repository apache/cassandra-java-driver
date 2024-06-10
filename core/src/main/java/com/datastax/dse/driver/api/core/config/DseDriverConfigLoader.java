/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.config;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.net.URL;

/**
 * @deprecated This class only exists for backward compatibility. All of its methods delegate to
 *     their counterparts on {@link DriverConfigLoader}, which you should call directly instead.
 */
@Deprecated
public class DseDriverConfigLoader {

  /**
   * @deprecated This method only exists for backward compatibility. It delegates to {@link
   *     DriverConfigLoader#fromClasspath(String)}, which you should call directly instead.
   */
  @Deprecated
  @NonNull
  public static DriverConfigLoader fromClasspath(@NonNull String resourceBaseName) {
    return DriverConfigLoader.fromClasspath(resourceBaseName);
  }

  /**
   * @deprecated This method only exists for backward compatibility. It delegates to {@link
   *     DriverConfigLoader#fromFile(File)}, which you should call directly instead.
   */
  @Deprecated
  @NonNull
  public static DriverConfigLoader fromFile(@NonNull File file) {
    return DriverConfigLoader.fromFile(file);
  }

  /**
   * @deprecated This method only exists for backward compatibility. It delegates to {@link
   *     DriverConfigLoader#fromUrl(URL)}, which you should call directly instead.
   */
  @Deprecated
  @NonNull
  public static DriverConfigLoader fromUrl(@NonNull URL url) {
    return DriverConfigLoader.fromUrl(url);
  }

  /**
   * @deprecated This method only exists for backward compatibility. It delegates to {@link
   *     DriverConfigLoader#programmaticBuilder()}, which you should call directly instead.
   */
  @Deprecated
  @NonNull
  public static ProgrammaticDriverConfigLoaderBuilder programmaticBuilder() {
    return DriverConfigLoader.programmaticBuilder();
  }

  private DseDriverConfigLoader() {
    throw new AssertionError("Not meant to be instantiated");
  }
}
