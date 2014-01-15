package net.sathis.export.sql.model;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.http.HttpException;

import com.mongodb.MongoException;

public class NoSQLWriter {

	private String primaryKey = null;
	
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void writeToNoSQL(List<Map<String, Object>> entityList) throws UnsupportedEncodingException, IOException, HttpException {
	}
	
	public void initConnection(ResourceBundle rb) throws UnknownHostException, MongoException, MalformedURLException, IOException {
	}
	
	public void close() {		
	}
	
}
