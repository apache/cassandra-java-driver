package com.datastax.driver.core;

import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class CountingTimestampGeneratorTest {
    @Test(groups = "unit")
    public void should_generate_incrementing_timestamps() {
        CountingTimestampGenerator generator = new CountingTimestampGenerator();
        long previous = (System.currentTimeMillis() - 1) * 1000;

        for (int i = 0; i < 1000; i++) {
            long timestamp = generator.next();
            assertTrue(timestamp > previous);
            previous = timestamp;
        }

        assertTrue((System.currentTimeMillis() + 1) * 1000 > previous);
    }
}
