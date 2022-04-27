/*
 * Copyright (C) 2022 ScyllaDB
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
 * Annotation for a Class or Method that skips it for Cassandra. If the tests are run against
 * Cassandra, the test is skipped.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CassandraSkip {
  /** @return The description returned if this requirement is not met. */
  String description() default "Disabled for Cassandra.";
}
