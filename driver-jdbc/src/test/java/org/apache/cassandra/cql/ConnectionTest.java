package org.apache.cassandra.cql;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class ConnectionTest {

	private static final String HOST = "lyn4e900.tlt--lyn4e901.tlt--lyn4e902.tlt";
	//private static final String HOST = "127.0.0.1";
    private static final int PORT = 9160;
    //private static final String KEYSPACE = "test?primaryDc=DC1";
    private static final String KEYSPACE = "system";
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
				Statement stmt = con.createStatement();
				//stmt.execute("select tournees from vision.evt where type_compteur='base' and date_chargement >= '2014-09-01'");
				stmt.execute("select code_tournee, informations from vision.info_tournee limit 5;");
				ResultSet result = stmt.getResultSet();
				if(result != null){							
						while(result.next()){
							System.out.println(result.getObject(1) + " - " + result.getObject("informations"));							
						}
				} 
				
				PreparedStatement prepStmt = con.prepareStatement("select no_lt, code_evt from vision.lt where no_lt = ?");
				prepStmt.setString(1, "DA380081542FR");
				prepStmt.execute();
				
				ResultSet result2 = prepStmt.getResultSet();
				//System.out.println(result2.);
				if(result2 != null){		
					System.out.println("pas nul : " + result2.toString());					
					while(result2.next()){
						System.out.println(result2.getObject(1) + " - " + result2.getString("no_lt"));							
					}
				}
				
				/*
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
