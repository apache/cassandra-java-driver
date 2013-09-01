package com.datastax.driver.orm.entity;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.Index;
import com.datastax.driver.core.orm.mapping.Key;


@ColumnFamily(name="drink",specificKeyspace="schemaA")
public class Drink implements Serializable {


    	private static final long serialVersionUID = -4750871137268349630L;

		@Key
	    private UUID id;
	    
	    @Index
	    @Column(name = "name")
	    private String name;
	    
	    @Column(name = "flavor")
	    private String flavor;

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getFlavor() {
			return flavor;
		}

		public void setFlavor(String flavor) {
			this.flavor = flavor;
		}
	    
	    
	    
	
}
