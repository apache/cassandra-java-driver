package com.datastax.driver.core;

import com.datastax.driver.core.Token.M3PToken;

public class M3PTokenIntegrationTest extends TokenIntegrationTest {
    public M3PTokenIntegrationTest() {
        super("", DataType.bigint());
    }

    @Override protected Token.Factory tokenFactory() {
        return M3PToken.FACTORY;
    }
}
