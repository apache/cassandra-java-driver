package com.datastax.driver.mapping;

import java.net.InetAddress;
import java.util.*;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.CCMBridge;

public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)",
                             "CREATE TABLE posts (user_id uuid, post_id timeuuid, title text, content text, device inet, PRIMARY KEY(user_id, post_id))");
    }

    @Test(groups = "short")
    public void testStaticEntity() throws Exception {
        Mapper m = new Mapper();

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        u1.setYear(2014);
        session.execute(m.save(u1));

        User read = m.map(session.execute(m.find(u1)), User.class).one();
        assertEquals(u1.getUserId(), read.getUserId());
        assertEquals(u1.getName(), read.getName());
        assertEquals(u1.getEmail(), read.getEmail());
        assertEquals(u1.getYear(), read.getYear());
        assertEquals(u1.getGender(), read.getGender());
    }

    @Test(groups = "short")
    public void testDynamicEntity() throws Exception {
        Mapper m = new Mapper();

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        Post p1 = new Post(u1, "Something about mapping");

        p1.setDevice(InetAddress.getLocalHost());

        session.execute(m.save(p1));

        Post read = m.map(session.execute(m.find(p1)), Post.class).one();
        assertEquals(p1.getUserId(), read.getUserId());
        assertEquals(p1.getPostId(), read.getPostId());
        assertEquals(p1.getTitle(), read.getTitle());
        assertEquals(p1.getContent(), read.getContent());
        assertEquals(p1.getDevice(), read.getDevice());
    }
}
