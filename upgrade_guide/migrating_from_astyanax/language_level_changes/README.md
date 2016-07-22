# Language change : from Thrift to CQL
The data model changes when using *CQL* (Cassandra Query Language).
*CQL* is providing an abstraction of the low-level data stored in *Cassandra*, in
opposition to *Thrift* that aims to expose the low-level data structure directly.
[But note that this changes with Cassandra 3’s new storage engine.](http://www.datastax.com/2015/12/storage-engine-30)

*Thrift* exposes *Keyspaces*, and these *Keyspaces* contain *Column Families*. A
*ColumnFamily* contains *Rows* in which each *Row* has a list of an arbitrary number
of column-values. With *CQL*, the data is **tabular**, *ColumnFamily* gets viewed
as a *Table*, the **Table Rows** get a **fixed and finite number of named columns**.
*Thrift*’s columns inside the *Rows* get distributed in a tabular way through the
_Table Rows_. See the following figure :

```ditaa
                                                     Thrift
             /-                                                                                          -\
             |                                                                                            |
             |  /------------\              /---------------+---------------+---------------+---------+   |
             |  |    cRED    |              |      Col 1    |      Col 2    |      Col 3    |         |   |
             |  | Key : 1    | ---------->  +---------------+---------------+---------------+ ...     |   +--> One Thrift
             |  |            |              |c1AB Val : 'a' |cFA0 Val : 'b' |     Val : 'c' |         |   |    ROW
             |  \------------/              \---------------+---------------+---------------+---------+   |
             |                                                                                            |
One Thrift   |                                                                                           -/
COLUMNFAMILY |          
             |  
             |  /------------\              /---------------+---------------+---------+
             |  |            |              |     Col 1     |     Col 4     |         |
             |  | Key : 2    | ---------->  +---------------+---------------+ ...     |
             |  |            |              |     Val : 'a' |     Val : 'b' |         |
             |  \------------/              \---------------+---------------+---------+
             |  
             \- 


                      -----------------------------------------------------------------------


                                                     CQL
                                                     
             /-                              
             |                               
             |  /--------------------+---------------------------+-----------------------+----------------------+-----------------------------\
             |  |       key          |           Col1            |         Col2          |         Col3         |            Col4             |  
             |  +--------------------+---------------------------+-----------------------+----------------------+-----------------------------+ -\
             |  | cRED   1           |   c1AB     'a'            | cFA0     'b'          |          'c'         |             null            |  +--> One CQL
   One CQL   |  +--------------------+---------------------------+-----------------------+----------------------+-----------------------------+ -/    ROW
   TABLE     |  |        2           |             'a'           |          null         |          null        |             'b'             |
             |  +--------------------+---------------------------+-----------------------+----------------------+-----------------------------+
             |  |       ...          |            ...            |          ...          |          ...         |             ...             |
             |  +--------------------+---------------------------+-----------------------+----------------------+-----------------------------+
             \-
```

Some of the columns of a *CQL Table* have a special role that is specifically
related to the *Cassandra* architecture. Indeed, the *Row key* of the *Thrift Row*,
becomes the *Partition Key* in the *CQL Table*, and can be composed of 1 or multiple
*CQL columns* (the key column in Figure 1). The *“Column”* part of the Column-value
component in a *Thrift Row*, becomes the *Clustering ColumnKey* in *CQL*, and can
also be composed of multiple columns (in the figure, column1 is the only column 
composing the *Clustering ColumnKey*).

Here is the basic architectural concept of *CQL*, a detailed explanation and *CQL*
examples can be found in this article : [http://www.planetcassandra.org/making-the-change-from-thrift-to-cql/].
Understanding the *CQL* abstraction plays a key role in developing performing
and scaling applications.
