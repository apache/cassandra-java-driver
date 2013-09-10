package com.datastax.driver.orm.dao;

import com.datastax.driver.core.orm.CassandraOperationsSimple;
import com.datastax.driver.core.orm.Persistence;
import com.datastax.driver.orm.entity.Book;

public class BookDAO extends CassandraOperationsSimple<Book, String>{
	
	public BookDAO(){
		persistence=CassandraFactoryUtil.INSTANCE.getPersistence();
	}
	private Persistence persistence;
	
	@Override
	protected Persistence getCassandraPersistence() {
		return persistence;
	}

}
