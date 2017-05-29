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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class StreamIdGeneratorTest {

    @Test(groups = "unit")
    public void SimpleGenIdTest() throws Exception {

        StreamIdGenerator generator = StreamIdGenerator.newInstance(ProtocolVersion.V2);

        assertEquals(generator.next(), 0);
        assertEquals(generator.next(), 64);
        generator.release(0);
        assertEquals(generator.next(), 0);
        assertEquals(generator.next(), 65);
        assertEquals(generator.next(), 1);
        generator.release(64);
        assertEquals(generator.next(), 64);
        assertEquals(generator.next(), 2);

        for (int i = 5; i < 128; i++)
            generator.next();

        generator.release(100);
        assertEquals(generator.next(), 100);

        assertEquals(generator.next(), -1);
    }
}
