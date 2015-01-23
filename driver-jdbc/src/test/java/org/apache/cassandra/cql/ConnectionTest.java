package org.apache.cassandra.cql;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class ConnectionTest {

	//private static final String HOST = "lyn4e900.tlt--lyn4e901.tlt--lyn4e902.tlt";
	private static final String HOST = "127.0.0.1";
    private static final int PORT = 9042;
    //private static final String KEYSPACE = "test?primaryDc=DC1";
    private static final String KEYSPACE = "testks";
    private static final String USER = "";
    private static final String PASSWORD = "";
    private static final String VERSION = "3.0.0";
    private static final String CONSISTENCY = "ONE";
    
    private static java.sql.Connection con = null;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");			
			for(int i=0;i<1;i++){
				con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
				DatabaseMetaData meta = con.getMetaData();
				//System.out.println(meta.getTableTypes());
				ResultSet result = meta.getTableTypes();
				
				while(result.next()){
					boolean ok =true;
					int x=1;
					while(ok){
						try{
							System.out.println(result.getString(x));
						}catch(Exception e){
							ok =false;
						}
						x++;
					}
					//System.out.println("Size : " + catalogs.getFetchSize());
				}
				
				System.out.println("meta.getCatalogs()");
				result = meta.getCatalogs();
				while(result.next()){
					boolean ok =true;
					int x=1;
					while(ok){
						try{
							System.out.println(result.getString(x));
						}catch(Exception e){
							ok =false;
						}
						x++;
					}
					//System.out.println("Size : " + catalogs.getFetchSize());
				}
				
				System.out.println("meta.getSchemas()");
				result = meta.getSchemas();
				while(result.next()){
					boolean ok =true;
					int x=1;
					while(ok){
						try{
							System.out.println(result.getString(x));
						}catch(Exception e){
							ok =false;
						}
						x++;
					}
					//System.out.println("Size : " + catalogs.getFetchSize());
				}
				System.out.println("meta.getSchemas(testks2)");
				result = meta.getSchemas("Test Cluster","testks2");
				while(result.next()){
					boolean ok =true;
					int x=1;
					while(ok){
						try{
							System.out.println(result.getString(x));
						}catch(Exception e){
							ok =false;
						}
						x++;
					}
					//System.out.println("Size : " + catalogs.getFetchSize());
				}
				
				System.out.println("meta.getTables()");
				result = meta.getTables(null,null,null,null);
				while(result.next()){
					boolean ok =true;
					int x=1;
					while(ok){
						try{
							System.out.println(result.getString(x));
						}catch(Exception e){
							ok =false;
						}
						x++;
					}
					//System.out.println("Size : " + catalogs.getFetchSize());
				}
				
				System.out.println("meta.getColumns()");
				result = meta.getColumns(null,null,null,null);
				while(result.next()){
					boolean ok =true;
					int x=1;
					while(ok){
						try{
							System.out.println(result.getString(x));
						}catch(Exception e){
							ok =false;
						}
						x++;
					}
					//System.out.println("Size : " + catalogs.getFetchSize());
				}
				
				
				
				/* Statement stmt = con.createStatement();
				//PreparedStatement prep = con.prepareStatement("SELECT  date_heure_fin , date_heure_debut ,    id_echange, id_etapes , nom_echange ,  etapes_vues ,  status_echange ,  attributs, duree  ,  etapes_attendues, warning, blobAsBigint(timestampAsBlob(date_heure_debut)), blobAsBigint(timestampAsBlob(date_heure_fin))  FROM fluks_dev.echange_ins limit 1");
				PreparedStatement prep = con.prepareStatement("SELECT  date_heure_fin  FROM fluks_dev.echange_ins limit 1");
				//stmt.execute("select * from dmas_envois_mas");
				//prep.setObject(1,"toto", Types.VARCHAR);
				ResultSet result = prep.executeQuery();
				ResultSetMetaData meta = result.getMetaData();
				System.out.println("colonnes : " + meta.getColumnCount());
				for(int j=1;j<=meta.getColumnCount();j++){
					try{
						System.out.println("col:" + meta.getColumnName(j));
					}catch(Exception e){
						System.out.println("j>>" + j);
						e.printStackTrace();
					}
				}
				
				
				if(result != null){
					if(!(result==null)){			
						while(result.next()){
							for(int j=1;j<=meta.getColumnCount();j++){
								System.out.println("col:" + meta.getColumnName(j));
								System.out.println("val:" + result.getString(meta.getColumnName(j)));
							}
							
						}
					}
				} */
			

				con.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
