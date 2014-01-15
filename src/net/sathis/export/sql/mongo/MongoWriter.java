package net.sathis.export.sql.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import net.sathis.export.sql.model.NoSQLWriter;
import net.sf.json.JSONArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class MongoWriter extends NoSQLWriter {
	
	DBCollection collection = null;
	
	private DB db;
	
	public DB getDB() {
		return db;
	}
	
	private static Log log = LogFactory.getLog(MongoWriter.class);
	
	@Override
	public void initConnection(ResourceBundle rb) throws UnknownHostException, MongoException {
		if (rb.getString("mongo.useAuth").equalsIgnoreCase("true")) {
			initConnection(rb.getString("mongo.host"), rb.getString("mongo.db") ,
					rb.getString("mongo.user"), rb.getString("mongo.password"));
		} else {
			initConnection( rb.getString("mongo.host"), rb.getString("mongo.db") );
		}
		initCollection(rb.getString("mongo.collection"));
	}
	
	private void initCollection(String name) {
		collection = getDB().getCollection(name);
	}
	
	public void initConnection(String url, String dbName) throws UnknownHostException, MongoException {
		Mongo m = new Mongo(url);
		db = m.getDB(dbName);
	}
	public void initConnection(String url, String dbName, String user, String password) throws UnknownHostException, MongoException {
		Mongo m = new Mongo(url);
		db = m.getDB(dbName);
		
		if (!db.authenticate(user, password.toCharArray())) {
			log.error("Couldn't Authenticate MongoDB!!!!!!.........");
			throw new MongoException("Couldn't Authenticate !!!!!!.........");
		}
	}
	
	@Override
	public void writeToNoSQL(List<Map<String, Object>> entityList) {
		JSONArray array = JSONArray.fromObject(entityList);
		List<DBObject> list = new ArrayList<DBObject>();
		DBObject object = null;
		for (int i = 0; i < array.size(); i++) {
			object = (DBObject) JSON.parse(array.get(i).toString());
			list.add(object);
		}
		if (list.size() > 0) {
			long  t1 = System.currentTimeMillis();
			collection.insert(list);
			long t2 = System.currentTimeMillis();
			log.info("Time taken to Write "+ list.size() + " documents to NoSQL :" + ((t2-t1))  + " ms");
		}
			
}
}
