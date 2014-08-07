package net.sathis.export.sql;

import java.util.Locale;
import java.util.ResourceBundle;

public class SQLToNoSQLImporter {


  public static void main(String[] args) throws Exception {
    
      ResourceBundle rb = ResourceBundle.getBundle("import", Locale.US);
      DataImporter importer = new DataImporter(rb);
      importer.setAutoCommitSize(Integer.valueOf(rb.getString("autoCommitSize")));
      importer.doDataImport(rb.getString("sql-data-config-file"));
  }
  
}
