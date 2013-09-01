package com.datastax.driver.orm.entity;

import java.util.Map;

import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.Key;
import com.datastax.driver.core.orm.mapping.MapData;


@ColumnFamily(name="resumebook")
public class Book {

    @Key
    @Column(name="booksname")
    private String name;
    
    @MapData
    private Map<Long, String> chapterResume;

    public Map<Long, String> getChapterResume() {
        return chapterResume;
    }

    public void setChapterResume(Map<Long, String> chapterResume) {
        this.chapterResume = chapterResume;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
}
