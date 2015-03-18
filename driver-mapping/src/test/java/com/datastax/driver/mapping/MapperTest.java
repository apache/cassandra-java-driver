/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import java.net.InetAddress;
import com.datastax.driver.mapping.annotations.Table;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.mapping.MapperTest.User.Gender;
import com.datastax.driver.mapping.annotations.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

/**
 * Basic tests for the mapping module.
 */
public class MapperTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        // We'll allow to generate those create statement from the annotated entities later, but it's currently
        // a TODO
        return Arrays.asList("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)",
                             "CREATE TABLE posts (user_id uuid, post_id timeuuid, title text, content text, device inet, tags set<text>, PRIMARY KEY(user_id, post_id))");
    }

    /*
     * Annotates a simple entity. Not a whole lot to see here, all fields are
     * mapped by default (but there is a @Transcient to have a field non mapped)
     * to a C* column that have the same name than the field (but you can use @Column
     * to specify the actual column name in C* if it's different).
     *
     * Do note that we support enums (which are mapped to strings by default
     * but you can map them to their ordinal too with some @Enumerated annotation)
     *
     * And the next step will be to support UDT (which should be relatively simple).
     */
    @Table(keyspace="ks", name = "users",
           readConsistency="QUORUM",
           writeConsistency="QUORUM")
    public static class User {

        // Dummy constant to test that static fields are properly ignored
        public static final int FOO = 1;

        public enum Gender { FEMALE, MALE }

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;
        private String email;
        @Column // not strictly required, but we want to check that the annotation works without a name
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

    /*
     * Another annotated entity, but that correspond to a table that has a
     * clustering column. Note that if there is more than one clustering column,
     * the order must be specified (@ClusteringColumn(0), @ClusteringColumn(1), ...).
     * The same stands for the @PartitionKey.
     */
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

    /*
     * We actually have 2 concepts in the mapping module. The first is the
     * mapping of entities like User and Post above. From such annotated entity
     * you can get a Mapper object (see below), which allow to map the Row of
     * ResultSet to proper object, and that provide a few simple method like
     * save, delete and a simple get.
     *
     * But to remove a bit of boilerplate when you need more complex queries, we
     * also have the concept of Accesor, which is just a way to associate some
     * java method calls to queries. Note that you don't have to use those Accessor
     * if you don't want (and in fact, you can use the Accessor concept even if
     * you don't map any entity).
     */
    @Accessor
    public interface PostAccessor {
        // Note that for implementation reasons, this *needs* to be an interface.

        // The @Param below is because you can't get the name of parameters of methods
        // by reflection (you can only have their types), so you have to annotate them
        // if you want to give them proper names in the query. That being said, if you
        // don't have @Param annotation like in the 2 other method, we default to some
        // harcoded arg0, arg1, .... A big annoying, and apparently Java 8 will fix that
        // somehow, but well, not a huge deal.
        @Query("SELECT * FROM ks.posts WHERE user_id=:userId AND post_id=:postId")
        public Post getOne(@Param("userId") UUID userId,
                           @Param("postId") UUID postId);

        // Note that the following method will be asynchronous (it will use executeAsync
        // underneath) because it's return type is a ListenableFuture. Similarly, we know
        // that we need to map the result to the Post entity thanks to the return type.
        @Query("SELECT * FROM ks.posts WHERE user_id=?")
        @QueryParameters(consistency="QUORUM")
        public ListenableFuture<Result<Post>> getAllAsync(UUID userId);

        // The method above actually query stuff, but if a method is declared to return
        // a Statement, it will not execute anything, but just return you the BoundStatement
        // ready for execution. That way, you can batch stuff for instance (see usage below).
        @Query("UPDATE ks.posts SET content=? WHERE user_id=? AND post_id=?")
        public Statement updateContentQuery(String newContent, UUID userId, UUID postId);

        @Query("SELECT * FROM ks.posts")
        public Result<Post> getAll();
    }

    @Accessor
    public interface UserAccessor {
        // Demonstrates use of an enum as an accessor parameter
        @Query("UPDATE ks.users SET name=?, gender=? WHERE user_id=?")
        ResultSet updateNameAndGender(String name, Gender gender, UUID userId);
    }

    @Test(groups = "short")
    public void testStaticEntity() throws Exception {
        // Very simple mapping a User, saving and getting it. Note that here we
        // don't use the Accessor stuff since the queries we use are directly
        // supported by the Mapper object.
        Mapper<User> m = new MappingManager(session).mapper(User.class);

        User u1 = new User("Paul", "paul@yahoo.com", User.Gender.MALE);
        u1.setYear(2014);
        m.save(u1);

        // Do note that m.get() takes the primary key of what we want to fetch
        // in argument, it doesn't not take a User object because we don't proxy
        // objects `a la' SpringData/Hibernate. The reason for not doing that
        // is that we don't want to encourage read-before-write.
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

        // Creates the accessor proxy defined above
        PostAccessor postAccessor = manager.createAccessor(PostAccessor.class);

        // Note that getOne is really the same than m.get(), it's just there
        // for demonstration sake.
        Post p = postAccessor.getOne(p1.getUserId(), p1.getPostId());
        assertEquals(p, p1);

        Result<Post> r = postAccessor.getAllAsync(p1.getUserId()).get();
        assertEquals(r.one(), p1);
        assertEquals(r.one(), p2);
        assertEquals(r.one(), p3);
        assertTrue(r.isExhausted());

        // No argument call
        r = postAccessor.getAll();
        assertEquals(r.one(), p1);
        assertEquals(r.one(), p2);
        assertEquals(r.one(), p3);
        assertTrue(r.isExhausted());

        BatchStatement batch = new BatchStatement();
        batch.add(postAccessor.updateContentQuery("Something different", p1.getUserId(), p1.getPostId()));
        batch.add(postAccessor.updateContentQuery("A different something", p2.getUserId(), p2.getPostId()));
        manager.getSession().execute(batch);

        Post p1New = m.get(p1.getUserId(), p1.getPostId());
        assertEquals(p1New.getContent(), "Something different");
        Post p2New = m.get(p2.getUserId(), p2.getPostId());
        assertEquals(p2New.getContent(), "A different something");

        m.delete(p1);
        m.delete(p2);

        // Check delete by primary key too
        m.delete(p3.getUserId(), p3.getPostId());

        assertTrue(postAccessor.getAllAsync(u1.getUserId()).get().isExhausted());

        // Pass an enum constant as an accessor parameter
        UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);
        userAccessor.updateNameAndGender("Paule", User.Gender.FEMALE, u1.getUserId());
        Mapper<User> userMapper = manager.mapper(User.class);
        assertEquals(userMapper.get(u1.getUserId()).getGender(), User.Gender.FEMALE);

        // Test that an enum value can be unassigned through an accessor (set to null).
        userAccessor.updateNameAndGender("Paule", null, u1.getUserId());
        assertEquals(userMapper.get(u1.getUserId()).getGender(), null);
    }
}
