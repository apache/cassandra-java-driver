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
package com.datastax.oss.driver.internal.core.config;

import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public class MockTypedOptions {
  public static final TypedDriverOption<Integer> INT1 =
      new TypedDriverOption<>(MockOptions.INT1, GenericType.INTEGER);
  public static final TypedDriverOption<Integer> INT2 =
      new TypedDriverOption<>(MockOptions.INT2, GenericType.INTEGER);
}
