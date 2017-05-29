/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Basic tests for the mapping module.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class MapperTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        // We'll allow to generate those create statement from the annotated entities later, but it's currently
        // a TODO
        execute("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)",
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
    @Table(name = "users",
            readConsistency = "QUORUM",
            writeConsistency = "QUORUM")
    public static class User {

        // Dummy constant to test that static fields are properly ignored
        public static final int FOO = 1;

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;
        private String email;
        @Column // not strictly required, but we want to check that the annotation works without a name
        private int year;

        public User() {
        }

        public User(String name, String email) {
            this.userId = UUIDs.random();
            this.name = name;
            this.email = email;
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

        public void setYear(int year) {
            this.year = year;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            User that = (User) other;
            return MoreObjects.equal(userId, that.userId)
                    && MoreObjects.equal(name, that.name)
                    && MoreObjects.equal(email, that.email)
                    && MoreObjects.equal(year, that.year);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(userId, name, email, year);
        }
    }

    /*
     * Another annotated entity, but that correspond to a table that has a
     * clustering column. Note that if there is more than one clustering column,
     * the order must be specified (@ClusteringColumn(0), @ClusteringColumn(1), ...).
     * The same stands for the @PartitionKey.
     */
    @SuppressWarnings("unused")
    @Table(name = "posts")
    public static class Post {

        private String title;
        private String content;
        private InetAddress device;

        @ClusteringColumn
        @Column(name = "post_id")
        private UUID postId;

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;


        private Set<String> tags;

        public Post() {
        }

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

            Post that = (Post) other;
            return MoreObjects.equal(userId, that.userId)
                    && MoreObjects.equal(postId, that.postId)
                    && MoreObjects.equal(title, that.title)
                    && MoreObjects.equal(content, that.content)
                    && MoreObjects.equal(device, that.device)
                    && MoreObjects.equal(tags, that.tags);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(userId, postId, title, content, device, tags);
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
        @Query("SELECT * FROM posts WHERE user_id=:userId AND post_id=:postId")
        Post getOne(@Param("userId") UUID userId,
                    @Param("postId") UUID postId);

        // Note that the following method will be asynchronous (it will use executeAsync
        // underneath) because it's return type is a ListenableFuture. Similarly, we know
        // that we need to map the result to the Post entity thanks to the return type.
        @Query("SELECT * FROM posts WHERE user_id=?")
        @QueryParameters(consistency = "QUORUM")
        ListenableFuture<Result<Post>> getAllAsync(UUID userId);

        // The method above actually query stuff, but if a method is declared to return
        // a Statement, it will not execute anything, but just return you the BoundStatement
        // ready for execution. That way, you can batch stuff for instance (see usage below).
        @Query("UPDATE posts SET content=? WHERE user_id=? AND post_id=?")
        Statement updateContentQuery(String newContent, UUID userId, UUID postId);

        @Query("SELECT * FROM posts")
        Result<Post> getAll();

        @Query("SELECT * FROM posts")
        @QueryParameters(idempotent = true)
        Statement getAllAsStatementIdempotent();

        @Query("SELECT * FROM posts")
        @QueryParameters(idempotent = false)
        Statement getAllAsStatementNonIdempotent();

        @Query("SELECT * FROM posts")
        Statement getAllAsStatement();
    }

    @Test(groups = "short")
    public void testStaticEntity() throws Exception {
        // Very simple mapping a User, saving and getting it. Note that here we
        // don't use the Accessor stuff since the queries we use are directly
        // supported by the Mapper object.
        Mapper<User> m = new MappingManager(session()).mapper(User.class);

        User u1 = new User("Paul", "paul@yahoo.com");
        u1.setYear(2014);
        m.save(u1);

        // Do note that m.get() takes the primary key of what we want to fetch
        // in argument, it doesn't not take a User object because we don't proxy
        // objects `a la' SpringData/Hibernate. The reason for not doing that
        // is that we don't want to encourage read-before-write.
        assertEquals(m.get(u1.getUserId()), u1);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void testDynamicEntity() throws Exception {
        MappingManager manager = new MappingManager(session());

        Mapper<Post> m = manager.mapper(Post.class);

        User u1 = new User("Paul", "paul@gmail.com");
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

    }

    @Test(groups = "short")
    public void should_map_objects_from_partial_queries() throws Exception {
        MappingManager manager = new MappingManager(session());

        Mapper<Post> m = manager.mapper(Post.class);

        // Insert a few posts
        User u1 = new User("Paul", "paul@gmail.com");
        Post p1 = new Post(u1, "Something about mapping");
        Post p2 = new Post(u1, "Something else");
        Post p3 = new Post(u1, "Something more");

        p1.setDevice(InetAddress.getLocalHost());
        p2.setTags(new HashSet<String>(Arrays.asList("important", "keeper")));

        m.save(p1);
        m.save(p2);
        m.save(p3);

        // Retrieve posts with a projection query that only retrieves some of the fields
        ResultSet rs = session().execute("select user_id, post_id, title from posts where user_id = " + u1.getUserId());

        Result<Post> result = m.map(rs);
        for (Post post : result) {
            assertThat(post.getUserId()).isEqualTo(u1.getUserId());
            assertThat(post.getPostId()).isNotNull();
            assertThat(post.getTitle()).isNotNull();

            assertThat(post.getDevice()).isNull();
            assertThat(post.getTags()).isNull();
        }

        // cleanup
        session().execute("delete from posts where user_id = " + u1.getUserId());
    }

    @Test(groups = "short")
    public void should_return_table_metadata() throws Exception {
        MappingManager manager = new MappingManager(session());

        Mapper<Post> m = manager.mapper(Post.class);

        assertThat(m.getTableMetadata()).isNotNull();
        assertThat(m.getTableMetadata().getName()).isEqualTo("posts");
        assertThat(m.getTableMetadata().getPartitionKey()).hasSize(1);
    }

    @Test(groups = "short")
    public void should_not_initialize_session_when_protocol_version_provided() {
        Session newSession = cluster().newSession();

        // Ensures that a Session is not initialized when a protocol version is provided.
        MappingManager manager = new MappingManager(newSession, ProtocolVersion.V1);
        assertThat(newSession.getState().getConnectedHosts()).hasSize(0);

        // Session should be initialized on first query.
        newSession.execute("USE " + keyspace);
        assertThat(newSession.getState().getConnectedHosts()).hasSize(1);
    }

    /**
     * Ensures that if an accessor method has a {@link QueryParameters} annotation with
     * {@link QueryParameters#idempotent()} as {@code true} that the {@link Statement} it generates returns
     * {@code true} for {@link Statement#isIdempotent()}.
     *
     * @jira_ticket JAVA-923
     * @test_category object_mapper
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_flag_statement_as_idempotent() {
        MappingManager manager = new MappingManager(session());
        PostAccessor post = manager.createAccessor(PostAccessor.class);
        Statement stmt = post.getAllAsStatementIdempotent();
        assertThat(stmt.isIdempotent()).isEqualTo(true);
    }

    /**
     * Ensures that if an accessor method has a {@link QueryParameters} annotation with
     * {@link QueryParameters#idempotent()} as {@code false} that the {@link Statement} it generates returns
     * {@code false} for {@link Statement#isIdempotent()}.
     *
     * @jira_ticket JAVA-923
     * @test_category object_mapper
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_flag_statement_as_non_idempotent() {
        MappingManager manager = new MappingManager(session());
        PostAccessor post = manager.createAccessor(PostAccessor.class);
        Statement stmt = post.getAllAsStatementNonIdempotent();
        assertThat(stmt.isIdempotent()).isEqualTo(false);
    }

    /**
     * Ensures that if an accessor method lacks a {@link QueryParameters} annotation with
     * {@link QueryParameters#idempotent()} set that the {@link Statement} it generates returns
     * {@code null} for {@link Statement#isIdempotent()}.
     *
     * @jira_ticket JAVA-923
     * @test_category object_mapper
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_flag_statement_with_null_idempotence() {
        MappingManager manager = new MappingManager(session());
        PostAccessor post = manager.createAccessor(PostAccessor.class);
        Statement stmt = post.getAllAsStatement();
        assertThat(stmt.isIdempotent()).isNull();
    }

    /**
     * Ensures that all statements generated by the mapper are
     * flagged as idempotent.
     *
     * @jira_ticket JAVA-923
     * @test_category object_mapper
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_flag_all_mapper_generated_statements_as_idempotent() {
        MappingManager manager = new MappingManager(session());
        Mapper<User> mapper = manager.mapper(User.class);
        User u = new User("Paul", "paul@yahoo.com");
        Statement saveQuery = mapper.saveQuery(u);
        assertThat(saveQuery.isIdempotent()).isTrue();
        Statement getQuery = mapper.getQuery(u.getUserId());
        assertThat(saveQuery.isIdempotent()).isTrue();
        Statement deleteQuery = mapper.deleteQuery(u.getUserId());
        assertThat(saveQuery.isIdempotent()).isTrue();
    }


    @Table(name = "users")
    public static class UserUnknownColumn {

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        @Column(name = "middle_name")
        private String middleName;

        public UserUnknownColumn() {
        }

        public UUID getUserId() {
            return userId;
        }

        public void setUserId(UUID userId) {
            this.userId = userId;
        }

        public String getMiddleName() {
            return middleName;
        }

        public void setMiddleName(String middleName) {
            this.middleName = middleName;
        }
    }

    /**
     * Ensures that when attempting to create a {@link Mapper} from a class that has a field for a
     * column that doesn't exist that an {@link IllegalArgumentException} is thrown.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short", expectedExceptions = {IllegalArgumentException.class})
    public void should_fail_to_create_mapper_if_class_has_column_not_in_table() {
        MappingManager manager = new MappingManager(session());
        manager.mapper(UserUnknownColumn.class);
    }

    @Table(name = "nonexistent")
    public static class NonExistentTable {
        public String name;

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    /**
     * Ensures that when attempting to create a {@link Mapper} from a class that has a {@link Table} annotation with
     * a name that doesn't exist in the current keyspace that an {@link IllegalArgumentException} is thrown.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short", expectedExceptions = {IllegalArgumentException.class})
    public void should_fail_to_create_mapper_if_table_does_not_exist() {
        MappingManager manager = new MappingManager(session());
        manager.mapper(NonExistentTable.class);
    }
}
