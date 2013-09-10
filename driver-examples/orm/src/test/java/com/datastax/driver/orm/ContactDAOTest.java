package com.datastax.driver.orm;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.orm.CassandraORMOperations;
import com.datastax.driver.orm.dao.ContactDAO;
import com.datastax.driver.orm.entity.Contact;

public class ContactDAOTest {
	
	private static final String ID = "otavio";
	private CassandraORMOperations<Contact, String> persistence=new ContactDAO();
	
	
	@Test
	public void saveTest(){
		Contact contact=new Contact();
		contact.setName(ID);
		Set<String> emails=new HashSet<String>();
		emails.add("teste@teste.com.br");
		emails.add("teste2@teste.com.br");
		contact.setEmails(emails);
		contact.setName(ID);
		Assert.assertNotNull(persistence.save(contact));
	}
	
	@Test
	public void findOneTest(){
		Contact contact=persistence.findOne(ID);
		Assert.assertNotNull(contact);
		Assert.assertTrue(contact.getEmails().size()==2);
	}
	
}
