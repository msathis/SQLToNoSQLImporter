SQLToNoSQLImporter
==================


INTRODUCTION : 

	SQLToNoSQLImporter is a Solr like data import handler to import Sql (MySQL,Oracle,PostgreSQL) data to NoSQL Systems (Mongodb,CouchDB,Elastic Search). 
	Migration of data from Sql to Mongodb or couchDB or Elasticsearch is now quite simple. Just write a configuration to import the data, this tool will export the data to your preferred NoSQL system. 
	SQLToNoSQLImporter reads from sql databases, converts and then inserts them into NoSQL datastore.For this purpose it uses one properties file (import.properties) where NoSQL datastore related settings are listed and one xml file with sql database related settings ,de-normalized schema,fields.
	For more info http://wiki.apache.org/solr/DataImportHandler#Configuration_in_data-config.xml

But the configuration file of SQLToNoSQLImporter varies slightly from solr's.


PROJECT STRUCTURE:
	1. conf/ 	- all configuration files.
	2. lib/		- all libraries (.jar files) and  JDBC Driver for your SQL Database
	3. src/		- Source code of this project


HOW TO RUN :

	1. change sql-db related settings in db-data-config.xml
	2. change NoSQL datastore related settings in import.properties
	3. run run.sh in linux by issuing command ./run.sh, in windows just double click run.bat file.
	
CONFIGURATIONS :
	
   import.properties
		
	NoSQL data store related settings.
	
   db-data-config.xml

	Most of the things are same as Solr's dataimport configuration file.For more info about Solr's configuration elements.
	   http://wiki.apache.org/solr/DataImportHandler

	The root entity's name will be used as collection name. If collection is not there, then it will be created. Field name which is given in the value 			of root entity's pk attribute will be used as primary key for the collection.

	Lets list The differences between our configuration file and Solr's dataimport configuration file
	
	1. Solr uses two configuration files (schema.xml,db-data-config.xml).SQLToNoSQLImporter uses only one configuration file.(db-data-config.xml).
	
	2. Field definitions have one more attribute "type".It is a mandatory one. Possible values are (STRING, INTEGER, DOUBLE, LONG, DATE, BOOLEAN).
	
	3. Entity definitions have one more optional attribut "multiValued".Possible values are (true, false).
	
	4. In Solr fields are multivalued.But in SQLToNoSQLImporter entities are multivalued.I don't see any usecase for having fields as multivalued (?).

	5. a) If one multivalued entity has only one field it will be converted to array list.
		"tags" : [ "mongodb", "couchdb", "hbase", "cassandra", "riak"]
	   b) If one multivalued entity has more than one field it will be converted to array list of objects.
		"tags" : [ { "name" : "mongodb" , "type" : "document"}, { "name" : "couchdb" , "type" : "document"},
			   { "name" : "hbase" , "type" : "bigtable"}, { "name" : "cassandra" , "type" : "bigtable"}, 
			   { "name" : "riak" , "type" : "key-value"}]
TODO :

	1. Support more NoSQL databases (HBase and Cassandra?).


