package com.datastax.driver.orm.entity;

import java.io.Serializable;

import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.CustomData;
import com.datastax.driver.core.orm.mapping.Key;

@ColumnFamily(name="picture")
public class Picture {

    @Key
    private String name;
    
    @Column(name="details")
    @CustomData
    private Details detail;

    
    
    public String getName() {
        return name;
    }



    public void setName(String name) {
        this.name = name;
    }



    public Details getDetail() {
        return detail;
    }



    public void setDetail(Details detail) {
        this.detail = detail;
    }



    public static class Details implements Serializable{
        
		private static final long serialVersionUID = 1447137274400838209L;

		private String fileName;
        
        private byte[] contents;

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public byte[] getContents() {
            return contents;
        }

        public void setContents(byte[] contents) {
            this.contents = contents;
        }
        
        
        
    }
}
