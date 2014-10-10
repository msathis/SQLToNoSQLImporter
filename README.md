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
	4. Run the project by issuing mvn test
	5. Or you can pass configuration files through command line like  mvn test -DimportConf=/Users/sathis/Desktop/data-import.properties -DdbConf=/Users/sathis/Desktop/data-config.xml 
	
**CONFIGURATIONS :**
	
   src/main/resources/import.properties
		
	NoSQL data store related settings.
	
   src/main/resources/db-data-config.xml

	Data conversion configuration. Similar to Solr's DIH.
			   

**ISSUES:**
    
    Please feel free to file the issues you encounter.


