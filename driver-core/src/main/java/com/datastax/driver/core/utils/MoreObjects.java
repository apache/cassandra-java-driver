/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core.utils;

import java.util.Arrays;

/**
 * Driver-specific implementation of utility object methods.
 * <p>
 * They are available in some versions of Java/Guava, but not across all versions ranges supported by the driver, hence
 * the custom implementation.
 */
public class MoreObjects {
    public static boolean equal(Object first, Object second) {
        return (first == second) || (first != null && first.equals(second));
    }

    public static int hashCode(Object... objects) {
        return Arrays.hashCode(objects);
    }
}
