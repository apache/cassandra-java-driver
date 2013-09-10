/*
 * Copyright 2013 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datastax.driver.core.orm.mapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import com.datastax.driver.core.orm.exception.DefaultCustmomizableException;

/**
 * The contract to custmo a storage way, you use it with the annotation CustomData 
 * @author osantana
 */
public interface Customizable {

	/**
	 * Read the Object and then return the bytebuffer what represents an objects
	 * @param object
	 * @return a stream of object
	 */
	ByteBuffer read(Object object);
	
	/**
	 * with the bytebuffer write a Object
	 * @param byteBuffer
	 * @return
	 */
	Object write(ByteBuffer byteBuffer);
	
	/**
	 * The default implementation, when uses the CustomData annotation without defines a class which implements this interface,
	 * The Cassandra ORM gonna to use it.
	 * A custom way to serialize and desserialize a object
	 * @author osantana
	 *
	 */
	 class DefaultCustmomizable implements Customizable{

		public ByteBuffer read(Object object) {
			
	    try {
	    		isSerializable(object);
	    	    ByteArrayOutputStream stream=new ByteArrayOutputStream();
				ObjectOutputStream storeObject = new ObjectOutputStream(stream);
				storeObject.writeObject(object);
				storeObject.flush();
				storeObject.close();
				
				return ByteBuffer.wrap(stream.toByteArray());
			} catch (IOException exception) {
				Logger.getLogger(DefaultCustmomizable.class.getName()).severe("An error heppend when DefaultCustmomizable try read or serialize the object "+object.getClass().getName()+" "+exception.getMessage());
			}
		
			
			return null;
		}

		private void isSerializable(Object object) {
		if(!(object instanceof Serializable)){
			StringBuilder mensageErro=new StringBuilder();
			mensageErro.append("the class ").append(object.getClass().getName());
			mensageErro.append(" should implements java.io.Serializable");
			throw new DefaultCustmomizableException(mensageErro.toString());
		}
			
		}

		public Object write(ByteBuffer byteBuffer) {
			
			
			try {
			    byte[] result = new byte[byteBuffer.remaining()];
			    byteBuffer.get(result);
				InputStream inputStream=new ByteArrayInputStream(result);
				ObjectInputStream objLeitura = new ObjectInputStream(inputStream);
				return objLeitura.readObject();
			} catch (Exception exception) {
				Logger.getLogger(DefaultCustmomizable.class.getName()).severe("An error heppend when DefaultCustmomizable try write and deserialize an object "+exception.getMessage());
			}
			return null;
		}
		}
}
