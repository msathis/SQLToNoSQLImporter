package net.sathis.export.sql.mongo;

import java.net.UnknownHostException;
import java.util.*;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.util.JSON;
import net.sathis.export.sql.model.NoSQLWriter;

import net.sf.json.JSONArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MongoWriter extends NoSQLWriter {

	List<MongoCollection> collections = new ArrayList<MongoCollection>();

	private static int count = 0;
	
	private MongoDatabase db;
	
	public MongoDatabase getDB() {
		return db;
	}
	
	private static Log log = LogFactory.getLog(MongoWriter.class);
	
	@Override
	public void initConnection(ResourceBundle rb) throws UnknownHostException, MongoException {
//		if (rb.getString("mongo.useAuth").equalsIgnoreCase("true")) {
//			initConnection(rb.getString("mongo.host"), rb.getString("mongo.port"), rb.getString("mongo.db") ,
//					rb.getString("mongo.user"), rb.getString("mongo.password"), rb.getString("mongo.options"));
//		} else {
//			initConnection( rb.getString("mongo.host"), rb.getString("mongo.port"), rb.getString("mongo.db"), rb.getString("mongo.options") );
//		}
        initConnection(rb.getString("mongo.uri"), rb.getString("mongo.db"));
		initCollections(rb.getString("mongo.collection").split(","));
	}

	private void initCollections(String[] name) {

		for (String coll : name){
			collections.add(getDB().getCollection(coll));
		}
	}

	public void initConnection(String uri, String dbname) throws UnknownHostException, MongoException {
	    MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        db = mongoClient.getDatabase(dbname);
    }
	
	public void initConnection(String url, String port, String dbName, String options) throws UnknownHostException, MongoException {
	    String[] listOfHosts = url.split(",");
	    List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
	    for (String host : listOfHosts){
	        serverAddressList.add(new ServerAddress(host, Integer.parseInt(port)));
        }
		MongoClient m = new MongoClient(serverAddressList);
		db = m.getDatabase(dbName);
	}

	public void initConnection(String url, String port, String dbName, String user, String password, String options) throws UnknownHostException, MongoException {
        String[] listOfHosts = url.split(",");
        List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
        for (String host : listOfHosts){
            serverAddressList.add(new ServerAddress(host, Integer.parseInt(port)));
        }
        MongoCredential mongoCredential = MongoCredential.createCredential(user,dbName,password.toCharArray());
        List<MongoCredential> mongoCredentialList = new ArrayList<MongoCredential>();
        mongoCredentialList.add(mongoCredential);
        MongoClient m = new MongoClient(serverAddressList);
        db = m.getDatabase(dbName);
	}

	@Override
	public void writeToNoSQL(List<Map<String, Object>> entityList) {
		JSONArray array = JSONArray.fromObject(entityList);
		List<DBObject> list = new ArrayList<DBObject>();
		Map<DBObject, DBObject> listOfExistingObj = new HashMap<DBObject, DBObject>();
		DBObject object = null;
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
		for (int i = 0; i < array.size(); i++) {
			object = (DBObject) JSON.parse(array.get(i).toString());
			list.add(object);
		}
		if (list.size() > 0) {
			long  t1 = System.currentTimeMillis();
			if (count < collections.size()){
				for (DBObject ob : list) {
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.append("$set", ob);
					BasicDBObject dbObject = new BasicDBObject().append("id",ob.get("id"));
					collections.get(count).updateMany(dbObject, newDocument, updateOptions);
				}
                long t2 = System.currentTimeMillis();
                log.info("Time taken to Write "+ list.size() + " documents to NoSQL :" + ((t2-t1))  + " ms, for Collection :" +collections.get(count));
                count++;
			}
			list = null;  // Free objects
			entityList = null;  // Free objects
		}

	}
}
