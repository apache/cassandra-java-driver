Driver JDBC
===========

This is the jdbc module of the DataStax Java Driver for Apache Cassandra (C*), 
which offers a simple JDBC compliant API to work with CQL3. 


Features
--------

The JDBC module offers access to most of the core module features:
  - Asynchronous: the driver uses the new CQL binary protocol asynchronous
    capabilities. Only a relatively low number of connections per nodes needs to
    be maintained open to achieve good performance.
  - Nodes discovery: the driver automatically discovers and uses all nodes of the
    C* cluster, including newly bootstrapped ones.
  - Configurable load balancing: the driver allows for custom routing and load
    balancing of queries to C* nodes. Out of the box, round robin is provided
    with optional data-center awareness (only nodes from the local data-center
    are queried (and have connections maintained to)) and optional token
    awareness (that is, the ability to prefer a replica for the query as coordinator).
  - Transparent fail-over: if C* nodes fail or become unreachable, the driver
    automatically and transparently tries other nodes and schedules
    reconnection to the dead nodes in the background.
  - Convenient schema access: the driver exposes a C* schema in a usable way.
  

Prerequisite
------------

The driver uses Casandra's native protocol which is available starting from
Cassandra 1.2. Some of the features (result set paging, BatchStatement, ...) of
this version 2.0 of the driver require Cassandra 2.0 however and will throw
'unsupported feature' exceptions if used against a Cassandra 1.2 cluster.

If you are having issues connecting to the cluster (seeing ``NoHostAvailableConnection``
exceptions) please check the `connection requirements <https://github.com/datastax/java-driver/wiki/Connection-requirements>`_.

If you want to run the unit tests provided with this driver, you will also need
to have ccm installed (http://github.com/pcmanus/ccm) as the tests use it. Also
note that the first time you run the tests, ccm will download/compile the
source of C* under the hood, which may require some time (that depends on your
Internet connection or machine).


Installing
----------

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>2.1.0</version>
    </dependency>
    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-jdbc</artifactId>
      <version>2.1.0</version>
    </dependency>



Getting Started
---------------

Connect to a Cassandra cluster using the following arguments::

    JDBC driver class : com.datastax.driver.jdbc.CassandraDriver
    JDBC URL : jdbc:cassandra://host1--host2--host3:9042/keyspace


    	
You can give the driver any number of host you want seperated by "--". 
They will be used as contact points for the driver to discover the entire cluster.
Give enough hosts taking into account that some nodes may be unavailable upon establishing the JDBC connection.

Statements and prepared statements can be executed as with any JDBC driver, but queries must be expressed in CQL3.

Java sample::

    Class.forName("com.datastax.driver.jdbc.CassandraDriver");
    String URL = "jdbc:cassandra://host1--host2--host3:9042/keyspace1";
    connection = DriverManager.getConnection(URL); 


Using simple statements
-------------------------

To issue a simple select and get data from it:: 

    statement = connection.createStatement();
    ResultSet result = statement.executeQuery("SELECT bValue,iValue FROM test_table WHERE keyname='key0';");
    while(result.next()){
        System.out.println("bValue = " + result.getBoolean("bValue"));
        System.out.println("iValue = " + result.getInt("iValue"));
    };


Using Prepared statements
-------------------------

Considering the following table:: 

    CREATE TABLE table1 
        (bigint_col bigint PRIMARY KEY, ascii_col ascii , blob_col blob, boolean_col boolean, 
        decimal_col decimal, double_col double, float_col float, inet_col inet, int_col int, 
        text_col text, timestamp_col timestamp, uuid_col uuid, 
        timeuuid_col timeuuid, varchar_col varchar, varint_col varint,string_set_col set<text>,
        string_list_col list<text>, string_map_col map<text,text>
        );


Prepared statements to insert a record in "table1":: 

    String insert = "INSERT INTO table1(bigint_col , ascii_col , blob_col , boolean_col , decimal_col , double_col , "
                    + "float_col , inet_col , int_col , text_col , timestamp_col , uuid_col , timeuuid_col , varchar_col , varint_col, string_set_col, string_list_col, string_map_col) "
                    + " values(?, ?, ?, ?, ?, ? , ?, ? , ? , ?, ? , ? , now(), ? , ?, ?, ?, ? );";
    
    PreparedStatement pstatement = connection.prepareStatement(insert);
    
    
    pstatement.setObject(1, 1L); // bigint
    pstatement.setObject(2, "test"); // ascii                             
    pstatement.setObject(3, new ByteArrayInputStream("test".getBytes("UTF-8"))); // blob
    pstatement.setObject(4, true); // boolean
    pstatement.setObject(5, new BigDecimal(5.1));  // decimal
    pstatement.setObject(6, (double)5.1);  // decimal
    pstatement.setObject(7, (float)5.1);  // inet
    InetAddress inet = InetAddress.getLocalHost();
    pstatement.setObject(8, inet);  // inet
    pstatement.setObject(9, (int)1);  // int
    pstatement.setObject(10, "test");  // text
    pstatement.setObject(11, new Timestamp(now.getTime()));  // text
    UUID uuid = UUID.randomUUID();
    pstatement.setObject(12, uuid );  // uuid
    pstatement.setObject(13, "test");  // varchar
    pstatement.setObject(14, 1);        
    HashSet<String> mySet = new HashSet<String>();
    mySet.add("test");
    mySet.add("test");
    pstatement.setObject(15, mySet);
    ArrayList<String> myList = new ArrayList<String>();
    myList.add("test");
    myList.add("test");
    pstatement.setObject(16, myList);
    HashMap<String,String> myMap = new HashMap<String,String>();
    myMap.put("1","test");
    myMap.put("2","test");
    pstatement.setObject(17, myMap);
            
    pstatement.execute();


Using Async Queries
-------------------

**INSERT/UPDATE**

There are 2 ways to insert/update data using asynchronous queries.
The first is to use JDBC batches (we're not talking about Cassandra atomic batches here).

With simple statements::

    Statement statement = con.createStatement();
    for(int i=0;i<10;i++){
        statement.addBatch("INSERT INTO testcollection (k,L) VALUES( " + i + ",[1, 3, 12345])");
    }
       
    int[] counts = statement.executeBatch();
    statement.close();

With prepared statements::

    PreparedStatement statement = con.prepareStatement("INSERT INTO testcollection (k,L) VALUES(?,?)");
        
    for(int i=0;i<10;i++){
        statement.setInt(1, i);
        statement.setString(2, "[1, 3, 12345]");
        statement.addBatch();
    }
        
    int[] counts = statement.executeBatch();
    statement.close();



The second one is to put all the queries in a single CQL statement, each ended with a semi colon (;)::

    Statement statement = con.createStatement();
            
    StringBuilder queryBuilder = new StringBuilder();        
    for(int i=0;i<10;i++){
        queryBuilder.append("INSERT INTO testcollection (k,L) VALUES( " + i + ",[1, 3, 12345]);");
    }
        
    statement.execute(queryBuilder.toString());
    statement.close();


**SELECT**

As JDBC batches do not support returning result sets, there is only one way to send asynchronous selects through the JDBC driver::

    StringBuilder queries = new StringBuilder();
    for(int i=0;i<10;i++){
        queries.append("SELECT * FROM testcollection where k = "+ i + ";");
    }
    
    //send all select queries at onces
    ResultSet result = statement.executeQuery(queries.toString());

    int nbRow = 0;
    ArrayList<Integer> ids = new ArrayList<Integer>(); 

    // get all results from all the select queries in a single result set
    while(result.next()){        
        ids.add(result.getInt("k"));
    }
