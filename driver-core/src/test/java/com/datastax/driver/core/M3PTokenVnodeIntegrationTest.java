package com.datastax.driver.core;

public class M3PTokenVnodeIntegrationTest extends TokenIntegrationTest {
    public M3PTokenVnodeIntegrationTest() {
        super("", DataType.bigint(), true);
    }

    @Override protected Token.Factory tokenFactory() {
        return Token.M3PToken.FACTORY;
    }
}
