package com.datastax.driver.mapping;

import java.net.InetAddress;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.mapping.annotations.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)",
                             "CREATE TABLE posts (user_id uuid, post_id timeuuid, title text, content text, device inet, tags set<text>, PRIMARY KEY(user_id, post_id))");
    }

    @Table(keyspace="ks", name = "users",
           readConsistency="QUORUM",
           writeConsistency="QUORUM")
    public static class User {

        public enum Gender { FEMALE, MALE }

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;
        private String email;
        private int year;

        private Gender gender;

        public User() {}

        public User(String name, String email, Gender gender) {
            this.userId = UUIDs.random();
            this.name = name;
            this.email = email;
            this.gender = gender;
        }

        public UUID getUserId() {
            return userId;
        }

        public void setUserId(UUID userId) {
            this.userId = userId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public int getYear() {
            return year;
        }

        public void setGender(Gender gender) {
            this.gender = gender;
        }

        public Gender getGender() {
            return gender;
        }

        public void setYear(int year) {
            this.year = year;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            User that = (User)other;
            return Objects.equal(userId, that.userId)
                && Objects.equal(name, that.name)
                && Objects.equal(email, that.email)
                && Objects.equal(year, that.year)
                && Objects.equal(gender, that.gender);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(userId, name, email, year, gender);
        }
    }

    @Table(keyspace = "ks", name = "posts")
    public static class Post {

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        @ClusteringColumn
        @Column(name = "post_id")
        private UUID postId;

        private String title;
        private String content;
        private InetAddress device;

        private Set<String> tags;

        public Post() {}

        public Post(User user, String title) {
            this.userId = user.getUserId();
            this.postId = UUIDs.timeBased();
            this.title = title;
        }

        public UUID getUserId() {
            return userId;
        }

        public void setUserId(UUID userId) {
            this.userId = userId;
        }

        public UUID getPostId() {
            return postId;
        }

        public void setPostId(UUID postId) {
            this.postId = postId;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public InetAddress getDevice() {
            return device;
        }

        public void setDevice(InetAddress device) {
            this.device = device;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTags(Set<String> tags) {
            this.tags = tags;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            Post that = (Post)other;
            return Objects.equal(userId, that.userId)
                && Objects.equal(postId, that.postId)
                && Objects.equal(title, that.title)
                && Objects.equal(content, that.content)
                && Objects.equal(device, that.device)
                && Objects.equal(tags, that.tags);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(userId, postId, title, content, device, tags);
        }
    }

    @Accessor
    public interface PostAccessor {

        @Query("SELECT * FROM ks.posts WHERE user_id=:userId AND post_id=:postId")
        public Post getOne(@Param("userId") UUID userId,
                           @Param("postId") UUID postId);

        @Query("SELECT * FROM ks.posts WHERE user_id=:arg0")
        @QueryParameters(consistency="QUORUM")
        public ListenableFuture<Result<Post>> getAllAsync(UUID userId);

        @Query("UPDATE ks.posts SET content=:arg2 WHERE user_id=:arg0 AND post_id=:arg1")
        public Statement updateContentQuery(UUID userId, UUID postId, String newContent);
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

        BatchStatement batch = new BatchStatement();
        batch.add(accessor.updateContentQuery(p1.getUserId(), p1.getPostId(), "Something different"));
        batch.add(accessor.updateContentQuery(p2.getUserId(), p2.getPostId(), "A different something"));
        manager.getSession().execute(batch);

        Post p1New = m.get(p1.getUserId(), p1.getPostId());
        assertEquals(p1New.getContent(), "Something different");
        Post p2New = m.get(p2.getUserId(), p2.getPostId());
        assertEquals(p2New.getContent(), "A different something");

        m.delete(p1);
        m.delete(p2);

        // Check delete by primary key too
        m.delete(p3.getUserId(), p3.getPostId());

        assertTrue(accessor.getAllAsync(u1.getUserId()).get().isExhausted());
    }
}
