package com.datastax.driver.core.orm;

import com.datastax.driver.core.Session;

 class RemoveAll {

	 public <T> void truncate(Class<T> bean, Session session){
			StringBuilder query=new StringBuilder();
			query.append("TRUNCATE ").append(ColumnUtil.INTANCE.getColumnFamilyNameSchema(bean)).append(";");
			session.execute(query.toString());
		}
}
