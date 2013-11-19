package com.datastax.driver.mapping;

import java.util.*;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.CCMBridge;

public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE a (c1 text PRIMARY KEY)");
    }

    @Test(groups = "short")
    public void testBasicEntity() throws Exception {
        Mapper m = new Mapper();

        A a = new A();
        a.setP1("abc");
        session.execute(m.save(a));

        a = m.map(session.execute(m.find(a)), A.class).one();
        assertEquals(a.getP1(), "abc");
    }
}
