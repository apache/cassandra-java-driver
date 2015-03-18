package com.datastax.driver.core;

import com.datastax.driver.core.Token.RPToken;

public class RPTokenIntegrationTest extends TokenIntegrationTest {
    public RPTokenIntegrationTest() {
        super("-p RandomPartitioner", DataType.varint());
    }

    @Override protected Token.Factory tokenFactory() {
        return RPToken.FACTORY;
    }
}
