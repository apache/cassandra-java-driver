package com.datastax.driver.orm.dao;

import com.datastax.driver.core.orm.CassandraOperationsSimple;
import com.datastax.driver.core.orm.Persistence;
import com.datastax.driver.orm.entity.Contact;

public class ContactDAO extends CassandraOperationsSimple<Contact, String>{

	public ContactDAO(){
		persistence=CassandraFactoryUtil.INSTANCE.getPersistence();
	}
	
	private Persistence persistence;
	
	@Override
	protected Persistence getCassandraPersistence() {
		return persistence;
	}

}
