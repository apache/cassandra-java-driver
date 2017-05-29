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
package com.datastax.driver.core;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
public @interface CreateCCM {

    enum TestMode {

        /**
         * When this mode is selected, one single CCM cluster will
         * be shared for all tests in the test class.
         * <p/>
         * When this mode is selected, only class-level {@link CCMConfig} annotations
         * are processed; method-level annotations are ignored.
         * <p/>
         * This mode usually runs faster, but care should be taken
         * not to alter the CCM cluster in a test in such a way
         * that subsequent tests could fail.
         */
        PER_CLASS,

        /**
         * When this mode is selected, a different CCM cluster
         * will be used for each test in the test class.
         * <p/>
         * When this mode is selected, both class-level and method-level {@link CCMConfig} annotations
         * are processed; the test configuration results from the merge of
         * both annotations, if both are present (method-level annotations
         * override class-level ones).
         * <p/>
         * This mode is slower, but is safer to use
         * if a test method alters the CCM cluster.
         */
        PER_METHOD
    }

    /**
     * The test mode to use for tests in this class.
     *
     * @return The test mode to use for tests in this class.
     */
    TestMode value() default TestMode.PER_CLASS;
}
