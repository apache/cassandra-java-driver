package com.datastax.driver.mapping;

import java.net.InetAddress;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.*;

import com.datastax.driver.core.utils.UUIDs;

@Table(name = "posts")
public class Post {

    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;

    @ClusteringColumn
    @Column(name = "post_id")
    private UUID postId;

    private String title;
    private String content;
    private InetAddress device;

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
}
