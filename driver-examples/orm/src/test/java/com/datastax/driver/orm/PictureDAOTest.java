package com.datastax.driver.orm;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.orm.CassandraORMOperations;
import com.datastax.driver.orm.dao.PictureDAO;
import com.datastax.driver.orm.entity.Picture;
import com.datastax.driver.orm.entity.Picture.Details;

public class PictureDAOTest {
	  private static final String ID = "mypicture";
	private CassandraORMOperations<Picture,String> dao = new PictureDAO();
	    
	    
	    @Test
	    public void insertTest(){
	        Picture picture=new Picture();
	        picture.setDetail(new Details());
	        picture.setName(ID);
	        picture.getDetail().setFileName("otavio.png");
	        byte[] file=new byte[200];
	        new Random().nextBytes(file);
	        picture.getDetail().setContents(file);
	        Assert.assertTrue(dao.save(picture) != null);
	        
	    }
	    
	    @Test
	    public void retriveTest(){
	        Picture picture=dao.findOne(ID);
	        Assert.assertNotNull(picture);
	    }
}
