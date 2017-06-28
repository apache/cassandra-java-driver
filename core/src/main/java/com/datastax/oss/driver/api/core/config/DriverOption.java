/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.config;

/** Describes an option in the driver's configuration. */
public interface DriverOption {
  String getPath();

  boolean required();

  /**
   * Concatenates two options to build a longer path.
   *
   * <p>This is intended for options that can appear at different levels, for example arguments of
   * policies that can be nested.
   */
  default DriverOption concat(DriverOption child) {
    DriverOption parent = this;
    // Not particularly efficient, but this will mainly be used for policies, which initialize at
    // startup, so it should be good enough.
    return new DriverOption() {
      @Override
      public String getPath() {
        return parent.getPath() + "." + child.getPath();
      }

      @Override
      public boolean required() {
        // This property is only for initial validation of the configuration, and we can't validate
        // nested fields because they are by definition not known in advance.
        return false;
      }
    };
  }
}
