package net.sathis.export.sql;

import net.sathis.export.sql.couch.CouchWriter;
import net.sathis.export.sql.es.ESWriter;
import net.sathis.export.sql.model.DataConfig;
import net.sathis.export.sql.model.DataStoreType;
import net.sathis.export.sql.model.NoSQLWriter;
import net.sathis.export.sql.mongo.MongoWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.naming.directory.InvalidAttributesException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;



public class DataImporter {

	private static Log log = LogFactory.getLog(DataImporter.class);
	
	private DataConfig config;
	
	private NoSQLWriter writer;
	
	private DataStoreType dataStoreType;
	
	int autoCommitSize = 500;
	
	int getAutoCommitSize() {
		return autoCommitSize;
	}
	
	void setAutoCommitSize(int newSize) {
		autoCommitSize = newSize;
	}
	
	DataConfig getConfig() {
		    return config;
    }
	
	public void setWriter(NoSQLWriter writer) {
		this.writer = writer;
	}

	public NoSQLWriter getWriter() {
		return writer;
	}
	
	private void findDataStoreWriter() throws InvalidAttributesException {
		if (getDataStoreType().equals(DataStoreType.MONGO)) {
			writer = new MongoWriter();
		} else if (getDataStoreType().equals(DataStoreType.ES)) {
			writer = new ESWriter();
		} else if(getDataStoreType().equals(DataStoreType.COUCH)) { 
		  writer = new CouchWriter();
		} else {
			throw new InvalidAttributesException("The requested datastore support is not available !.");
		}
	}

	public DataImporter(ResourceBundle rb) throws Exception {
		dataStoreType = DataStoreType.valueOf(rb.getString("dataStoreType").toUpperCase());
		findDataStoreWriter();
		getWriter().initConnection(rb);
	}

    private String getXmlFile(String path) {
        if (new File(path).exists()) {
            return path;
        }
        return  Thread.currentThread().getContextClassLoader().getResource(path).getFile();
    }

	public void doDataImport(String path) {
			InputSource file = new InputSource(getXmlFile(path));
		  	loadDataConfig(file);
		  	
			if (config != null) {
				for (DataConfig.Entity e : config.document.entities) {
				      Map<String, DataConfig.Field> fields = new HashMap<String, DataConfig.Field>();
				      initEntity(e, fields, false);
				      identifyPk(e);
				}
				doFullImport();
			} else {
				log.error("Configuration files are missing !!!!!!!!.......");
			}
	  }
	  
	  private void identifyPk(DataConfig.Entity entity) {
		  
		    String schemaPk = "";
		    entity.pkMappingFromSchema = schemaPk;
		    
		    for (DataConfig.Field field : entity.fields) {
		      if(field.getName().equals(schemaPk)) {
		        entity.pkMappingFromSchema = field.column;
		        break;
		      }
		    } 

	 }
	  
	  private  void loadDataConfig(InputSource configFile) {

		    try {
		      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		      
		      DocumentBuilder builder = dbf.newDocumentBuilder();
		      Document document;
		      try {
		        document = builder.parse(configFile);
		      } finally {
		        // some XML parsers are broken and don't close the byte stream (but they should according to spec)
		        IOUtils.closeQuietly(configFile.getByteStream());
		      }

		      config = new DataConfig();
		      NodeList elems = document.getElementsByTagName("dataConfig");
		      if(elems == null || elems.getLength() == 0) {
		    	  log.error("the root node '<dataConfig>' is missing");
		    	  throw new IOException();
		      }
		      	config.readFromXml((Element) elems.item(0));
		      	log.info("Data Configuration loaded successfully");
		    } catch (Exception e) {
		    	log.error(e.getStackTrace());
		  }

	  }

	private void initEntity(DataConfig.Entity e,
			Map<String, DataConfig.Field> fields, boolean docRootFound) {
		e.allAttributes.put(DATA_SRC, e.dataSource);

		if (!docRootFound && !"false".equals(e.docRoot)) {
			e.isDocRoot = true;
		}
		
		if (e.fields != null) {
			for (DataConfig.Field f : e.fields) {
				
				fields.put(f.getName(), f);
				f.entity = e;
				f.allAttributes.put("boost", f.boost.toString());
				f.allAttributes.put("toWrite", Boolean.toString(f.toWrite));
				e.allFieldsList.add(Collections
						.unmodifiableMap(f.allAttributes));
			}
		}
		e.allFieldsList = Collections.unmodifiableList(e.allFieldsList);
		e.allAttributes = Collections.unmodifiableMap(e.allAttributes);

		if (e.entities == null)
			return;
		for (DataConfig.Entity e1 : e.entities) {
			e1.parentEntity = e;
			initEntity(e1, fields, e.isDocRoot || docRootFound);
		}
	}
	
	 public void doFullImport() {
		try {
			DocBuilder docBuilder = new DocBuilder(this);
			docBuilder.execute();
			getWriter().close();
			log.info("*****  Data import completed successfully. **********");
		} catch (Throwable t) {
		  log.error("*****  Data import failed. **********\n Reason is :");
			t.printStackTrace();
		}
	  }
	
	public void setDataStoreType(DataStoreType exportType) {
		this.dataStoreType = exportType;
	}

	public DataStoreType getDataStoreType() {
		return dataStoreType;
	}



	public static final String COLUMN = "column";

	public static final String TYPE = "type";

	public static final String DATA_SRC = "dataSource";

}
