package com.datastax.driver.orm.dao;

import com.datastax.driver.core.orm.CassandraFactory;
import com.datastax.driver.core.orm.CassandraFactorySimple;
import com.datastax.driver.core.orm.Persistence;
import com.datastax.driver.core.orm.PersistenceSimpleImpl;
import com.datastax.driver.orm.entity.Book;
import com.datastax.driver.orm.entity.Contact;
import com.datastax.driver.orm.entity.Drink;
import com.datastax.driver.orm.entity.Engineer;
import com.datastax.driver.orm.entity.LinuxDistribuition;
import com.datastax.driver.orm.entity.Person;
import com.datastax.driver.orm.entity.Picture;
import com.datastax.driver.orm.entity.ProductShopping;
import com.datastax.driver.orm.entity.ShoppingList;

public enum CassandraFactoryUtil {
INSTANCE;

private CassandraFactorySimple cassandraFactory;

{
 cassandraFactory=new CassandraFactorySimple("localhost", "dataStax");
 
 cassandraFactory.addFamilyObject(Book.class);
 cassandraFactory.addFamilyObject(Contact.class);
 cassandraFactory.addFamilyObject(Drink.class);
 cassandraFactory.addFamilyObject(Engineer.class);
 cassandraFactory.addFamilyObject(LinuxDistribuition.class);
 cassandraFactory.addFamilyObject(ShoppingList.class);
 cassandraFactory.addFamilyObject(Person.class);
 cassandraFactory.addFamilyObject(Picture.class);
 cassandraFactory.addFamilyObject(ProductShopping.class);
 
}


public Persistence getPersistence(){
	return new PersistenceSimpleImpl(cassandraFactory);
}

public CassandraFactory getCassandraFactory(){
	return cassandraFactory;
}
}
