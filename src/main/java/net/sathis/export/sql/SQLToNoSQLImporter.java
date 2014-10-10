package net.sathis.export.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class SQLToNoSQLImporter {

  private static Log log = LogFactory.getLog(SQLToNoSQLImporter.class);

  public static void main(String[] args) throws Exception {

      String importConf = "import.properties", dbConf = "db-data-config.xml";

      importConf = args.length > 0 ? args[0] : importConf;
      dbConf = args.length > 1 ? args[1] : dbConf;

      log.info("Configuration files are 1. " + importConf + " , 2. " + dbConf);

      ResourceBundle rb = getResourceBundle(importConf, Locale.US);
      DataImporter importer = new DataImporter(rb);
      importer.setAutoCommitSize(Integer.valueOf(rb.getString("autoCommitSize")));
      importer.doDataImport(dbConf);
  }

    private static ResourceBundle getResourceBundle(String url, Locale locale) {
        try {
            FileInputStream file = new FileInputStream(url);
            return new PropertyResourceBundle(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResourceBundle.getBundle(url.split("\\.")[0], Locale.US);
    }
}
