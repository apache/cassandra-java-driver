package com.datastax.driver.mapping;

import java.util.UUID;

import com.google.common.base.Objects;

import com.datastax.driver.mapping.annotations.*;

import com.datastax.driver.core.utils.UUIDs;

@Table(keyspace="ks", name = "users",
       readConsistency="QUORUM",
       writeConsistency="QUORUM")
public class User {

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
