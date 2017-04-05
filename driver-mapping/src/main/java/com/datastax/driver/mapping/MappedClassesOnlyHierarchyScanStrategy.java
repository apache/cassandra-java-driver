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
package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.List;

/**
 * A {@link HierarchyScanStrategy} that excludes all ancestors of mapped classes, thus
 * restricting class scan to the mapped classes themselves.
 * <p>
 * This strategy can be used instead of {@link DefaultHierarchyScanStrategy} to
 * achieve pre-<a href="https://datastax-oss.atlassian.net/browse/JAVA-541">JAVA-541</a>
 * behavior.
 */
public class MappedClassesOnlyHierarchyScanStrategy implements HierarchyScanStrategy {

    @Override
    public List<Class<?>> filterClassHierarchy(Class<?> mappedClass) {
        return Collections.<Class<?>>singletonList(mappedClass);
    }
}
