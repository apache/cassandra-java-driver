package com.datastax.driver.core;

import org.junit.Test;
import static junit.framework.Assert.*;

public class StreamIdGeneratorTest {

    @Test
    public void SimpleGenIdTest() {

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
        } catch (IllegalStateException e) {
            // Ok, expected
        }
    }
}
