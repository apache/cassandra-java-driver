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
package com.datastax.dse.driver.internal.core.type.codec;

import static com.datastax.oss.driver.internal.core.util.Dependency.ESRI;

import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.util.DefaultDependencyChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseTypeCodecsRegistrar {

  private static final Logger LOG = LoggerFactory.getLogger(DseTypeCodecsRegistrar.class);

  public static void registerDseCodecs(MutableCodecRegistry registry) {
    registry.register(DseTypeCodecs.DATE_RANGE);
    if (DefaultDependencyChecker.isPresent(ESRI)) {
      registry.register(DseTypeCodecs.LINE_STRING, DseTypeCodecs.POINT, DseTypeCodecs.POLYGON);
    } else {
      LOG.debug("ESRI was not found on the classpath: geo codecs will not be available");
    }
  }
}
