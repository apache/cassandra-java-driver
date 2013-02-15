/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import org.junit.Test;
import static junit.framework.Assert.*;

public class StreamIdGeneratorTest {

    @Test
    public void SimpleGenIdTest() throws Exception {

        StreamIdGenerator generator = new StreamIdGenerator();

        assertEquals(0, generator.next());
        assertEquals(1, generator.next());
        generator.release(0);
        assertEquals(0, generator.next());
        assertEquals(2, generator.next());
        assertEquals(3, generator.next());
        generator.release(1);
        assertEquals(1, generator.next());
        assertEquals(4, generator.next());

        for (int i = 5; i < 128; i++)
            assertEquals(i, generator.next());

        generator.release(100);
        assertEquals(100, generator.next());

        try {
            generator.next();
            fail("No more streamId should be available");
        } catch (BusyConnectionException e) {
            // Ok, expected
        }
    }
}
