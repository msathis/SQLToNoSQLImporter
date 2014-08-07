package net.sathis.export.sql.es;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import net.sathis.export.sql.model.NoSQLWriter;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.FailedCommunicationException;

public class ESWriter extends NoSQLWriter {
	
	private static Log log = LogFactory.getLog(ESWriter.class);
	
	Client client = null;
	
	String index_name = null;
	
	String index_type = null;
	
	BulkRequestBuilder bulkRequest;
	
	@Override
	public void initConnection(ResourceBundle rb) {
		Settings settings = ImmutableSettings.settingsBuilder()
        	.put("cluster.name", rb.getString("es.cluster.name")).build();
		client = new TransportClient(settings);
		String host[] = StringUtils.split(rb.getString("es.hosts"), ",");
		for (int i = 0; i < host.length; i++) {
			((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(host[i], 9300));
		}
		bulkRequest = client.prepareBulk();
		setIndexProperties(rb.getString("es.index.name"), rb.getString("es.index.type"));
	}
	
	void setIndexProperties(String index_name, String index_type) {
		this.index_name = index_name;
		this.index_type = index_type;
	}
	
	@Override
	public void writeToNoSQL(List<Map<String, Object>> entityList) {
		JSONArray array = JSONArray.fromObject(entityList);
		for (int i = 0; i < array.size(); i++) {
			IndexRequestBuilder builder = client.prepareIndex(index_name, index_type);
			if (getPrimaryKey() != null)
				builder.setId( ((JSONObject)array.get(i)).getString(getPrimaryKey()));
			builder.setSource(array.get(i).toString());
			bulkRequest.add(builder);
		}
		if (bulkRequest.numberOfActions() > 0) {
			long  t1 = System.currentTimeMillis();
			ListenableActionFuture<BulkResponse>  action = bulkRequest.execute();
			long t2 = System.currentTimeMillis();
			BulkResponse response = action.actionGet();
		    for (Iterator<BulkItemResponse> iterator = response.iterator(); iterator.hasNext();) {
		      BulkItemResponse e = (BulkItemResponse) iterator.next();
		      if (e.isFailed()) 
		        throw new FailedCommunicationException("Insertion to ES failed.");
		    }
			log.info("Time taken to Write "+ bulkRequest.numberOfActions() + " documents to ES :" + ((t2-t1))  + " ms");
		}
	}
	
	@Override
	public void close() {
		client.close();
	}
}
