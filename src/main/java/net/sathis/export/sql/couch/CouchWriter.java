package net.sathis.export.sql.couch;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import net.sathis.export.sql.model.NoSQLWriter;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;

public class CouchWriter extends NoSQLWriter {
  
  DefaultHttpClient httpclient = null;
  
  private static Log log = LogFactory.getLog(CouchWriter.class);
  
  public String url;
  
  @Override
  public void initConnection(ResourceBundle rb) throws MalformedURLException,
      IOException {
    url = "http://" + rb.getString("couch.host") + ":"
        + rb.getString("couch.port") + "/" + rb.getString("couch.db")
        + "/_bulk_docs";
  }
  
  @Override
  public void writeToNoSQL(List<Map<String,Object>> entityList)
      throws UnsupportedEncodingException, IOException, HttpException {
    
    JSONArray array = JSONArray.fromObject(entityList);
    JSONObject object = new JSONObject();
    object.put("docs", array);
    
    if (array.size() > 0) {
      long t1 = System.currentTimeMillis();
      post(object.toString());
      long t2 = System.currentTimeMillis();
      log.info("Time taken to Write " + array.size()
          + " documents to CouchDB :" + ((t2 - t1)) + " ms");
    }
  }
  
  void post(String content) throws  IOException, HttpException {
    HttpPost post = new HttpPost(url);
    if (content != null) {
        httpclient = new DefaultHttpClient();
        HttpEntity entity = new StringEntity(content, ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        post.setHeader(new BasicHeader("Content-Type", "application/json"));
        HttpResponse response = httpclient.execute(post);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED)
          throw new HttpException(response.getStatusLine().toString());
    }
  }
  
}
