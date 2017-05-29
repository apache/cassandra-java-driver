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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.testng.annotations.DataProvider;

import java.util.Arrays;
import java.util.Iterator;

public class DataProviders {

    /**
     * @return A DataProvider that provides all non-serial consistency levels
     */
    @DataProvider(name = "consistencyLevels")
    public static Iterator<Object[]> consistencyLevels() {
        final Iterator<ConsistencyLevel> consistencyLevels = Iterables.filter(Arrays.asList(ConsistencyLevel.values()), new Predicate<ConsistencyLevel>() {
            @Override
            public boolean apply(ConsistencyLevel input) {
                // filter out serial CLs.
                return !input.isSerial();
            }
        }).iterator();

        return new Iterator<Object[]>() {

            @Override
            public boolean hasNext() {
                return consistencyLevels.hasNext();
            }

            @Override
            public Object[] next() {
                return new Object[]{
                        consistencyLevels.next()
                };
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("This shouldn't happen..");
            }
        };
    }

    /**
     * @return A DataProvider that provides all serial consistency levels
     */
    @DataProvider(name = "serialConsistencyLevels")
    public static Object[][] serialConsistencyLevels() {
        return new Object[][]{
                {ConsistencyLevel.SERIAL},
                {ConsistencyLevel.LOCAL_SERIAL}
        };
    }

}
