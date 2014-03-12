package com.datastax.driver.mapping;

import java.util.UUID;

import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.annotations.*;

@Accessor
public interface PostAccessor {

    @Query("SELECT * FROM ks.posts WHERE user_id=:userId AND post_id=:postId")
    public Post getOne(@Param("userId") UUID userId,
                       @Param("postId") UUID postId);

    @Query("SELECT * FROM ks.posts WHERE user_id=:arg0")
    @QueryOptions(consistency="QUORUM")
    public ListenableFuture<Result<Post>> getAllAsync(UUID userId);

    @Query("UPDATE ks.posts SET content=:arg2 WHERE user_id=:arg0 AND post_id=:arg1")
    public Statement updateContentQuery(UUID userId, UUID postId, String newContent);

}
