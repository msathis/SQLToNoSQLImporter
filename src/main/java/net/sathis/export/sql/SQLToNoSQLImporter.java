package net.sathis.export.sql;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class SQLToNoSQLImporter {

  public static void main(String[] args) throws Exception {

      String dbConf = args.length > 1 && args[1] != null ? args[1] : "db-data-config.xml";
      ResourceBundle rb = args.length > 0 && args[0] != null ? getResourceBundle(args[0], Locale.US)
              : getDefaultRB(Locale.US);

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
        return ResourceBundle.getBundle(url.split("\\.")[0], locale);
    }

    private static ResourceBundle getDefaultRB(Locale locale) {
        return ResourceBundle.getBundle("import", locale);
    }
}
