package com.datastax.driver.mapping;

import java.net.InetAddress;
import java.util.*;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.core.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)",
                             "CREATE TABLE posts (user_id uuid, post_id timeuuid, title text, content text, device inet, tags set<text>, PRIMARY KEY(user_id, post_id))");
    }

    @Test(groups = "short")
    public void testStaticEntity() throws Exception {
        Mapper<User> m = new MappingManager(session).mapper(User.class);

        User u1 = new User("Paul", "paul@yahoo.com", User.Gender.MALE);
        u1.setYear(2014);
        m.save(u1);

        assertEquals(m.get(u1.getUserId()), u1);
    }

    @Test(groups = "short")
    public void testDynamicEntity() throws Exception {
        MappingManager manager = new MappingManager(session);

        Mapper<Post> m = manager.mapper(Post.class);

        User u1 = new User("Paul", "paul@gmail.com", User.Gender.MALE);
        Post p1 = new Post(u1, "Something about mapping");
        Post p2 = new Post(u1, "Something else");
        Post p3 = new Post(u1, "Something more");

        p1.setDevice(InetAddress.getLocalHost());

        p2.setTags(new HashSet<String>(Arrays.asList("important", "keeper")));

        m.save(p1);
        m.save(p2);
        m.save(p3);

        PostAccessor accessor = manager.createAccessor(PostAccessor.class);

        Post p = accessor.getOne(p1.getUserId(), p1.getPostId());
        assertEquals(p, p1);

        Result<Post> r = accessor.getAllAsync(p1.getUserId()).get();
        assertEquals(r.one(), p1);
        assertEquals(r.one(), p2);
        assertEquals(r.one(), p3);
        assertTrue(r.isExhausted());

        m.delete(p1);
        m.delete(p2);

        // Check delete by primary key too
        m.delete(p3.getUserId(), p3.getPostId());

        assertTrue(accessor.getAllAsync(u1.getUserId()).get().isExhausted());
    }
}
