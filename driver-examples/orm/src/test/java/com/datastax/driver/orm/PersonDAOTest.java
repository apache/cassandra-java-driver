package com.datastax.driver.orm;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.orm.dao.PersonDAO;
import com.datastax.driver.orm.entity.Address;
import com.datastax.driver.orm.entity.Person;
import com.datastax.driver.orm.entity.Sex;

public class PersonDAOTest {

	private PersonDAO dao=new PersonDAO();
	
	   @Test
	    public void insertTest() {
	    	
	        Person person = getPerson();
	        person.setAddress(getAddress());
	        Assert.assertNotNull(dao.save(person));
	    }
	   
	   @Test
	    public void retrieveTest() {
		    Person person = getPerson();
		    person.setAddress(getAddress());
		    person.setId(10l);;
		    dao.save(person);
	        Person otherPerson = dao.findOne(10l);
	        Assert.assertNotNull(otherPerson.getName());
	    }

	    @Test
	    public void retriveSubClassTest() {
	        Person person = dao.findOne(4l);
	        Assert.assertNotNull(person.getAddress().getCity());
	    }

	    @Test
	    public void retrieveEnumTest() {
	        Person person = dao.findOne(4l);
	        Assert.assertEquals(person.getSex(), Sex.MALE);
	    }

	    @Test
	    public void retrieveEnumEmptyTest() {
	        Person person = dao.findOne(4l);
	        Assert.assertNotNull(person.getSex());
	    }

	    @Test
	    public void overrideTest() {

	        Person person = getPerson();
	        person.setId(1l);
	        Assert.assertTrue(dao.save(person) !=null);
	    }

	    @Test
	    public void removefromRowKeyTest() {
	    	dao.delete(new Long(2));
	        Assert.assertTrue(true);
	    }

	    @Test
	    public void removeTest() {
	        Person person = getPerson();
	        person.setId(1l);
	        Assert.assertTrue(dao.save(person) !=null);
	        dao.delete(person);
	        Assert.assertFalse(dao.exists(1l));
	    }

	    @Test
	    public void cantRetrieve() {
	        Person person = dao.findOne(new Long(-1));
	        Assert.assertNull(person);
	    }

	    @Test
	    public void listTest() {
	        Person person = getPerson();
	        person.setId(1l);
	        dao.save(person);
	        Assert.assertNotNull(dao.findAll());

	    }

	    @Test
	    public void listNotNull() {
	        Iterable<Person> persons = dao.findAll();

	        Assert.assertNotNull(persons);
	    }

	   

	    @Test
	    public void countNotNullTest() {

	        Assert.assertNotNull(dao.count());
	    }


	    @Test
	    public void findIndexTest(){
	    	List<Person> persons= dao.findByIndex("otavio","name");
	    	Assert.assertNotNull(persons);
	    }

	    @Test
	    public void executeUpdateCqlTest() {
	    }


	    @Test
	    public void countTest() {
	        Assert.assertTrue(dao.count()>0);
	    }
	    
	  private Address getAddress() {
	        Address address = new Address();
	        address.setCep("40243-543");
	        address.setCity("Salvaor");
	        address.setState("Bahia");
	        return address;
	    }

	    private Person getPerson() {
	        Person person = new Person();
	        person.setId(4l);
	        person.setYear(10);
	        person.setName("Otávio");
	        person.setSex(Sex.MALE);
	        return person;
	    }
}
