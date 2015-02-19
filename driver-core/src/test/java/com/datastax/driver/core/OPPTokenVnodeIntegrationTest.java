package com.datastax.driver.core;

public class OPPTokenVnodeIntegrationTest extends TokenIntegrationTest {
    public OPPTokenVnodeIntegrationTest() {
        super("-p ByteOrderedPartitioner", DataType.blob(), true);
    }

    @Override protected Token.Factory tokenFactory() {
        return Token.OPPToken.FACTORY;
    }
}
