package com.datastax.driver.core;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

public class SimpleStatementTest {
    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_fail_if_too_many_variables() {
        List<Object> args = Collections.nCopies(1 << 16, (Object)1);
        new SimpleStatement("mock query", args.toArray());
    }
}