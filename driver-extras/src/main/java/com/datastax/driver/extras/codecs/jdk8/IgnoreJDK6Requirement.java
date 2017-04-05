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
package com.datastax.driver.extras.codecs.jdk8;

/**
 * Annotation used to mark classes in this package as
 * excluded from JDK signature check performed
 * by <a href="http://www.mojohaus.org/animal-sniffer/animal-sniffer-maven-plugin/check-mojo.html">animal-sniffer</a>
 * Maven plugin as they require JDK 8 and not the usual JDK 6.
 */
@interface IgnoreJDK6Requirement {
}
