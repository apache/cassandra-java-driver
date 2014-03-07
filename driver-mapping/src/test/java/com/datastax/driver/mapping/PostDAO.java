package com.datastax.driver.mapping;

import java.util.UUID;

import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.*;

@DAO(keyspace = "ks", name = "posts")
public interface PostDAO {

    @Query("SELECT * FROM ks.posts WHERE user_id=:userId AND post_id=:postId")
    public Post getOne(@Param("userId") UUID userId,
                       @Param("postId") UUID postId);

    @Query("SELECT * FROM ks.posts WHERE user_id=:arg0")
    public ListenableFuture<Result<Post>> getAllAsync(UUID userId);
}
