package com.datastax.driver.core;

public class RPTokenVnodeIntegrationTest extends TokenIntegrationTest {
    public RPTokenVnodeIntegrationTest() {
        super("-p RandomPartitioner", DataType.varint(), true);
    }

    @Override protected Token.Factory tokenFactory() {
        return Token.RPToken.FACTORY;
    }
}
