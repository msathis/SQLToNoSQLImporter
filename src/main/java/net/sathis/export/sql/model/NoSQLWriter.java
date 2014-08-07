package net.sathis.export.sql.model;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

public class NoSQLWriter {

	private String primaryKey = null;
	
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void writeToNoSQL(List<Map<String, Object>> entityList) throws Exception {
	}
	
	public void initConnection(ResourceBundle rb) throws Exception {
	}
	
	public void close() {		
	}
	
}
