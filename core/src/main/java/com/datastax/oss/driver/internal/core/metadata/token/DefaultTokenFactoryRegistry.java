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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTokenFactoryRegistry implements TokenFactoryRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTokenFactoryRegistry.class);

  private final String logPrefix;

  public DefaultTokenFactoryRegistry(InternalDriverContext context) {
    this.logPrefix = context.clusterName();
  }

  @Override
  public TokenFactory tokenFactoryFor(String partitioner) {
    if (partitioner.endsWith("Murmur3Partitioner")) {
      LOG.debug("[{}] Detected Murmur3 partitioner ({})", logPrefix, partitioner);
      return new Murmur3TokenFactory();
    } else if (partitioner.endsWith("RandomPartitioner")) {
      LOG.debug("[{}] Detected random partitioner ({})", logPrefix, partitioner);
      return new RandomTokenFactory();
    } else if (partitioner.endsWith("OrderedPartitioner")) {
      LOG.debug("[{}] Detected byte ordered partitioner ({})", logPrefix, partitioner);
      return new ByteOrderedTokenFactory();
    } else {
      LOG.warn(
          "[{}] Unsupported partitioner '{}', token map will be empty.", logPrefix, partitioner);
      return null;
    }
  }
}
