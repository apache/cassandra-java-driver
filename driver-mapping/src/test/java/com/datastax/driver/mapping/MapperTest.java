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
        Mapper<User> m = new Mapper(session).mapperFor(User.class);

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        u1.setYear(2014);
        m.save(u1);

        User read = m.get(u1.getUserId());
        assertEquals(u1.getUserId(), read.getUserId());
        assertEquals(u1.getName(), read.getName());
        assertEquals(u1.getEmail(), read.getEmail());
        assertEquals(u1.getYear(), read.getYear());
        assertEquals(u1.getGender(), read.getGender());
    }

    @Test(groups = "short")
    public void testDynamicEntity() throws Exception {
        Mapper m = new Mapper(session).mapperFor(Post.class);

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        Post p1 = new Post(u1, "Something about mapping");
        Post p2 = new Post(u1, "Something else");

        p1.setDevice(InetAddress.getLocalHost());

        m.save(p1);
        m.save(p2);

        Result<Post> r = m.map(session.execute(m.select(Post.class).where(eq("user_id", u1.getUserId()))), Post.class);
        Post read1 = r.one();
        assertEquals(p1.getUserId(), read1.getUserId());
        assertEquals(p1.getPostId(), read1.getPostId());
        assertEquals(p1.getTitle(), read1.getTitle());
        assertEquals(p1.getContent(), read1.getContent());
        assertEquals(p1.getDevice(), read1.getDevice());

        Post read2 = r.one();
        assertEquals(p2.getUserId(), read2.getUserId());
        assertEquals(p2.getPostId(), read2.getPostId());
        assertEquals(p2.getTitle(), read2.getTitle());
        assertEquals(p2.getContent(), read2.getContent());
        assertEquals(p2.getDevice(), read2.getDevice());

        m.delete(p1.getUserId(), p1.getPostId());
        m.delete(p2.getUserId(), p2.getPostId());

        assertTrue(m.map(session.execute(m.select(Post.class).where(eq("user_id", u1.getUserId()))), Post.class).isExhausted());
    }
}
