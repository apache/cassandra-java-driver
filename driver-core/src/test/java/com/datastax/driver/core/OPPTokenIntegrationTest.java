package com.datastax.driver.core;

import com.datastax.driver.core.Token.OPPToken;

public class OPPTokenIntegrationTest extends TokenIntegrationTest {
    public OPPTokenIntegrationTest() {
        super("-p ByteOrderedPartitioner", DataType.blob());
    }

    @Override protected Token.Factory tokenFactory() {
        return OPPToken.FACTORY;
    }
}
