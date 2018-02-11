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
package com.datastax.oss.driver.api.testinfra;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation for a Class or Method that defines a Cassandra Version requirement. If the cassandra
 * version in use does not meet the version requirement, the test is skipped.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CassandraRequirement {

  /** @return The minimum version required to execute this test, i.e. "2.0.13" */
  String min() default "";

  /**
   * @return the maximum exclusive version allowed to execute this test, i.e. "2.2" means only tests
   *     &lt; "2.2" may execute this test.
   */
  String max() default "";

  /** @return The description returned if this version requirement is not met. */
  String description() default "Does not meet version requirement.";
}
