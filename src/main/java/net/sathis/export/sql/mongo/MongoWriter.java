package net.sathis.export.sql.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.sathis.export.sql.model.NoSQLWriter;
import org.bson.Document;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MongoWriter extends NoSQLWriter {

	MongoCollection collection = null;

	private MongoDatabase db;

	public MongoDatabase getDB() {
		return db;
	}

	private static Log log = LogFactory.getLog(MongoWriter.class);

	@Override
	public void initConnection(ResourceBundle rb) throws UnknownHostException, MongoException {
		if (!rb.getString("mongo.replicaSet").isEmpty()) {
			initConnection(rb.getString("mongo.host"), rb.getString("mongo.port"),
					rb.getString("mongo.db"), rb.getString("mongo.user"),
					rb.getString("mongo.password"), rb.getString("mongo.replicaSet"),
					rb.getString("mongo.authSource"));
		} else if (rb.getString("mongo.useAuth").equalsIgnoreCase("true")) {
			initConnection(rb.getString("mongo.host"), rb.getString("mongo.port"), rb.getString("mongo.db"),
					rb.getString("mongo.user"), rb.getString("mongo.password"));
		} else {
			initConnection(rb.getString("mongo.host"), rb.getString("mongo.port"), rb.getString("mongo.db"));
		}
		initCollection(rb.getString("mongo.collection"));
	}

	@Override
	public void initCollection(String name) {
		collection = getDB().getCollection(name);
	}

	/**
	 * No authentication
	 * @param url
	 * @param port
	 * @param dbName
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public void initConnection(String url, String port, String dbName) throws UnknownHostException, MongoException {
		String[] listOfHosts = url.split(",");
		List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
		for (String host : listOfHosts) {
			serverAddressList.add(new ServerAddress(host, Integer.parseInt(port)));
		}
		MongoClient m = new MongoClient(serverAddressList);
		db = m.getDatabase(dbName);
	}

	/**
	 * Username/Password authentication
	 * @param url
	 * @param port
	 * @param dbName
	 * @param user
	 * @param password
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public void initConnection(String url, String port, String dbName, String user, String password) throws UnknownHostException, MongoException {
		String[] listOfHosts = url.split(",");
		List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
		for (String host : listOfHosts) {
			serverAddressList.add(new ServerAddress(host, Integer.parseInt(port)));
		}
		MongoCredential mongoCredential = MongoCredential.createCredential(user, dbName, password.toCharArray());
		List<MongoCredential> mongoCredentialList = new ArrayList<MongoCredential>();
		mongoCredentialList.add(mongoCredential);
		MongoClient m = new MongoClient(serverAddressList, mongoCredentialList);
		db = m.getDatabase(dbName);
	}

	/**
	 * Replica Set with Username/Password authentication (Mongo Atlas)
	 * @param url
	 * @param port
	 * @param dbName
	 * @param user
	 * @param password
	 * @param replicaSet
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public void initConnection(String url, String port, String dbName, String user, String password, String replicaSet, String authSource) throws UnknownHostException, MongoException {
		String[] listOfHosts = url.split(",");
		List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
		for (String host : listOfHosts) {
			serverAddressList.add(new ServerAddress(host, Integer.parseInt(port)));
		}

		MongoCredential mongoCredential = MongoCredential.createCredential(user, authSource, password.toCharArray());
		List<MongoCredential> mongoCredentialList = new ArrayList<MongoCredential>();
		mongoCredentialList.add(mongoCredential);
		MongoClientOptions mongoOptions = MongoClientOptions.builder().sslEnabled(Boolean.TRUE).requiredReplicaSetName(replicaSet).build();
		MongoClient m = new MongoClient(serverAddressList, mongoCredentialList, mongoOptions);
		db = m.getDatabase(dbName);
	}

	@Override
	public void writeToNoSQL(List<Map<String, Object>> entityList) {
		List<Document> list = new ArrayList<Document>();
		Document object = null;
		for (int i = 0; i < entityList.size(); i++) {
			object = new Document(entityList.get(i));
			list.add(object);
		}
		if (list.size() > 0) {
			long t1 = System.currentTimeMillis();
			collection.insertMany(list);
			long t2 = System.currentTimeMillis();
			log.info("Time taken to Write " + list.size() + " documents to NoSQL :" + ((t2 - t1)) + " ms");
		}

	}
}
