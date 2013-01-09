package com.datastax.driver.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;

public class MapperTest {

    @Test
    public void testBasicEntity() throws Exception {

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("test");
        Mapper m = new Mapper();

        A a = new A();
        a.setP1("abc");
        session.execute(m.save(a));

        a = m.map(session.execute(m.find(a)), A.class).fetchOne();
        assertEquals("abc", a.getP1());
        session.shutdown();
    }
}
