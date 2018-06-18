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
package com.datastax.oss.driver.api.core.metadata;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Convenience class for listener implementations that that don't need to override all methods (all
 * methods in this class are empty).
 */
public class NodeStateListenerBase implements NodeStateListener {

  @Override
  public void onAdd(@NonNull Node node) {
    // nothing to do
  }

  @Override
  public void onUp(@NonNull Node node) {
    // nothing to do
  }

  @Override
  public void onDown(@NonNull Node node) {
    // nothing to do
  }

  @Override
  public void onRemove(@NonNull Node node) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
