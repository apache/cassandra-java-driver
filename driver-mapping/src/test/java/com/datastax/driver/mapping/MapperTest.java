package com.datastax.driver.mapping;

import java.net.InetAddress;
import java.util.*;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.CCMBridge;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)",
                             "CREATE TABLE posts (user_id uuid, post_id timeuuid, title text, content text, device inet, PRIMARY KEY(user_id, post_id))");
    }

    @Test(groups = "short")
    public void testStaticEntity() throws Exception {
        Mapper<User> m = new MappingManager(session).mapper(User.class);

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        u1.setYear(2014);
        m.save(u1);

        assertEquals(m.get(u1.getUserId()), u1);
    }

    @Test(groups = "short")
    public void testDynamicEntity() throws Exception {
        Mapper<Post> m = new MappingManager(session).mapper(Post.class);

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        Post p1 = new Post(u1, "Something about mapping");
        Post p2 = new Post(u1, "Something else");
        Post p3 = new Post(u1, "Something more");

        p1.setDevice(InetAddress.getLocalHost());

        m.save(p1);
        m.save(p2);
        m.save(p3);

        Result<Post> r = m.slice(u1.getUserId()).execute();

        assertEquals(r.one(), p1);
        assertEquals(r.one(), p2);
        assertEquals(r.one(), p3);

        r = m.slice(u1.getUserId()).from(p2.getPostId()).execute();

        assertEquals(r.one(), p2);
        assertEquals(r.one(), p3);

        m.delete(p1);
        m.delete(p2);

        // Check delete by primary key too
        m.delete(p3.getUserId(), p3.getPostId());

        assertTrue(m.slice(u1.getUserId()).execute().isExhausted());
    }
}
