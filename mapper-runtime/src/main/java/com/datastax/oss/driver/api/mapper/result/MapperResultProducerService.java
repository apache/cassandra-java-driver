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
package com.datastax.oss.driver.api.mapper.result;

/**
 * Provides the custom mapper result types that will be used in an application.
 *
 * <p>This class is loaded with the Java Service Provider Interface mechanism, you must reference it
 * via a service descriptor: create a file {@code
 * META-INF/services/com.datastax.oss.driver.api.mapper.result.MapperResultProducerService}, with
 * one or more lines, each referencing the name of an implementing class.
 */
public interface MapperResultProducerService {

  /**
   * Returns the producers provided by this service.
   *
   * <p>Note that order matters, the producers will be tried from left to right until one matches.
   * If there is some overlap between your producers' {@link MapperResultProducer#canProduce
   * canProduce()} implementations, put the most specific ones first.
   */
  Iterable<MapperResultProducer> getProducers();
}
