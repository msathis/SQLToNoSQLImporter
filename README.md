SQLToNoSQLImporter
==================


SQLToNoSQLImporter is a Solr like data import handler to import Sql (MySQL,Oracle,PostgreSQL) data to NoSQL Systems (Mongodb,CouchDB,Elastic Search). 
	Migration is now completely configuration driven. User is expected to write a configuration, this tool will export the data to the preferred NoSQL system. 
	SQLToNoSQLImporter reads from sql databases, converts and then batch inserts them into NoSQL datastore.For this purpose it uses one properties file (import.properties) where NoSQL datastore related settings are listed and one xml file with sql database related settings ,de-normalized schema,fields.
	For more info about [Solr's Data Import Handler!](http://wiki.apache.org/solr/DataImportHandler#Configuration_in_data-config.xml)

But the configuration file of SQLToNoSQLImporter varies slightly from solr's DIH.


**PROJECT STRUCTURE:**

	1. src/main/resources/ 	- All configuration files.
	2. src/main/java		 - Source code of this project

**HOW TO RUN :**

    1. change sql-db related settings in src/main/resources/db-data-config.xml
	2. change NoSQL datastore related settings in src/main/resources/import.properties
	3. Install and export maven to PATH.
	4. Run the project by issuing *mvn test*
	
**CONFIGURATIONS :**
	
   src/main/resources/import.properties
		
	NoSQL data store related settings.
	
   src/main/resources/db-data-config.xml

	Most of the things are same as Solr's dataimport configuration file.For more info about Solr's configuration elements.
	   http://wiki.apache.org/solr/DataImportHandler

	The root entity's name will be used as collection name. If collection is not there, then it will be created. Field name which is given in the value 			of root entity's pk attribute will be used as primary key for the collection.

	Lets list The differences between our configuration file and Solr's dataimport configuration file
	
	1. Solr uses two configuration files (schema.xml,db-data-config.xml).SQLToNoSQLImporter uses only one configuration file.(db-data-config.xml).
	
	2. Field definitions have one more attribute "type".It is a mandatory one. Possible values are (STRING, INTEGER, DOUBLE, LONG, DATE, BOOLEAN).
	
	3. Entity definitions have one or more optional attributes "multiValued".Possible values are (true, false).
	
	4. In Solr fields are multivalued. But in SQLToNoSQLImporter entities are multivalued.
	
	5. a) If one multivalued entity has only one field it will be converted to array list.
		"tags" : [ "mongodb", "couchdb", "hbase", "cassandra", "riak"]
	   b) If one multivalued entity has more than one field it will be converted to array list of objects.
		"tags" : [ { "name" : "mongodb" , "type" : "document"}, { "name" : "couchdb" , "type" : "document"},
			   { "name" : "hbase" , "type" : "bigtable"}, { "name" : "cassandra" , "type" : "bigtable"}, 
			   { "name" : "riak" , "type" : "key-value"}]
			   

**ISSUES:**
    
    Please feel free to file the issues you encounter.


