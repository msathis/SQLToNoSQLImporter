package net.sathis.export.sql;

import java.io.IOException;
import java.util.Locale;
import java.util.ResourceBundle;

import javax.naming.directory.InvalidAttributesException;

import com.mongodb.MongoException;

public class SQLToNoSQLImporter {


  public static void main(String[] args) throws MongoException, IOException, InvalidAttributesException {
    
      ResourceBundle rb = ResourceBundle.getBundle("import", Locale.US);
      DataImporter importer = new DataImporter(rb);
      importer.setAutoCommitSize(Integer.valueOf(rb.getString("autoCommitSize")));
      importer.doDataImport(rb.getString("sql-data-config-file"));
  }
  
}
