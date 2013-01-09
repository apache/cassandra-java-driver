package com.datastax.driver.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.CCMBridge;

public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Collections.singleton("CREATE TABLE a (c1 text PRIMARY KEY)");
    }

    @Test
    public void testBasicEntity() throws Exception {
        Mapper m = new Mapper();

        A a = new A();
        a.setP1("abc");
        session.execute(m.save(a));

        a = m.map(session.execute(m.find(a)), A.class).fetchOne();
        assertEquals("abc", a.getP1());
    }
}
