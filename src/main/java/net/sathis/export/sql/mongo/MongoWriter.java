package net.sathis.export.sql.mongo;

import java.net.UnknownHostException;
import java.util.*;

import com.mongodb.*;
import com.mongodb.util.JSON;
import net.sathis.export.sql.model.NoSQLWriter;

import net.sf.json.JSONArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MongoWriter extends NoSQLWriter {

	List<DBCollection> collections = new ArrayList<DBCollection>();

	private static int count = 0;
	
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
		initCollections(rb.getString("mongo.collection").split(","));
	}

	private void initCollections(String[] name) {

		for (String coll : name){
			collections.add(getDB().getCollection(coll));
		}
	}
	
	public void initConnection(String url, String dbName) throws UnknownHostException, MongoException {
		Mongo m = new Mongo(url);
		db = m.getDB(dbName);
	}

	public void initConnection(String url, String dbName, String user, String password) throws UnknownHostException, MongoException {
		Mongo m = new Mongo(url);
		db = m.getDB(dbName);
		db.setWriteConcern(WriteConcern.ERRORS_IGNORED);
		if (!db.authenticate(user, password.toCharArray())) {
			log.error("Couldn't Authenticate MongoDB!!!!!!.........");
			throw new MongoException("Couldn't Authenticate !!!!!!.........");
		}
	}

	@Override
	public void writeToNoSQL(List<Map<String, Object>> entityList) {
		JSONArray array = JSONArray.fromObject(entityList);
		List<DBObject> list = new ArrayList<DBObject>();
		Map<DBObject, DBObject> listOfExistingObj = new HashMap<DBObject, DBObject>();
		DBObject object = null;
		DBObject object1 = null;
		for (int i = 0; i < array.size(); i++) {
			object = (DBObject) JSON.parse(array.get(i).toString());
			list.add(object);
//			if (count < collections.size()){
//				object1 = (DBObject) collections.get(count).findOne(object);
//				if (object1 != null){
//					listOfExistingObj.put(object1,object);
//				}
//			}
		}
		if (list.size() > 0) {
			long  t1 = System.currentTimeMillis();
			if (count < collections.size()){
//				collections.get(count).insert(list);
//				for(DBObject dbObject : listOfExistingObj.keySet()){
//					collections.get(count).update(dbObject,listOfExistingObj.get(dbObject));
//				}
//				WriteConcern writeConcern = WriteConcern.ERRORS_IGNORED;
				//collections.get(count).insert(list);
				for (DBObject ob : list) {
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.append("$set", ob);
					BasicDBObject dbObject = new BasicDBObject().append("id",ob.get("id"));
					collections.get(count).update(dbObject, newDocument, true, false);
				}
				count++;
			}
			long t2 = System.currentTimeMillis();
			log.info("Time taken to Write "+ list.size() + " documents to NoSQL :" + ((t2-t1))  + " ms");
			list = null;  // Free objects
			entityList = null;  // Free objects
		}

	}
}
