package net.sathis.export.sql;

import net.sathis.export.sql.model.DataConfig.Entity;
import net.sathis.export.sql.model.DataConfig.Field;
import net.sathis.export.sql.model.DataStoreType;
import net.sathis.export.sql.model.FieldType;
import net.sathis.export.sql.model.FieldTypeParser;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;






/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



/**
 * <p> {@link DocBuilder} is responsible for creating Solr documents out of the given configuration. It also maintains
 * statistics information. It depends on the {@link EntityProcessor} implementations to fetch data. </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DocBuilder {
	
	private static Log log = LogFactory.getLog(DocBuilder.class);
	
	private DataImporter importer;
	
	private Connection conn = null;
	
	private Connection subConnection = null;
	
	private Statement stmt = null;
	
	ResultSet rs = null;
	
	ResultSet resultSet = null;
	
	private int batchSize = 100;
	
	private Map<String, String> params = null;
	
	private Map<String, Object> subEntityData = null;
	
	private Statement subLevel = null;
	
	private Pattern p = Pattern.compile("(\\$\\{.*?\\})");
	
	private Matcher m ;
	
	BasicRowProcessor processor = new BasicRowProcessor();
	
	public DocBuilder(DataImporter importer) {
		this.importer = importer;
	}
	
	public void execute() throws Exception {

		String url = importer.getConfig().dataSources.get(null).getProperty("url");
	    String user = importer.getConfig().dataSources.get(null).getProperty("user");
	    String password = importer.getConfig().dataSources.get(null).getProperty("password");
	    batchSize = Integer.valueOf(importer.getConfig().dataSources.get(null).getProperty("batch-size"));
	    Entity rootEntity = importer.getConfig().document.entities.get(0);
	    String driverName = importer.getConfig().dataSources.get(null).getProperty(DRIVER);
	   
	    //Set the fetch size depending on SQL type
	    if (batchSize == -1 && driverName.contains("mysql"))
	        batchSize = Integer.MIN_VALUE;
	    else if (batchSize == -1)
	    	  batchSize = 0;
	    
	    Class.forName(driverName).newInstance();
	    conn = DriverManager.getConnection(url, user, password);
	    subConnection = DriverManager.getConnection(url, user, password);
	    
	    //set the PK for future use
	    importer.getWriter().setPrimaryKey(rootEntity.pk);
	   
	    if (rootEntity != null && rootEntity.isDocRoot) {
	    	String rootQuery = rootEntity.allAttributes.get("query");
	    	stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	    	stmt.setMaxRows(0);
	    	stmt.setFetchSize(batchSize);
	    	rs = stmt.executeQuery(rootQuery);
	    	int i = 0;
	    	List<Map<String, Object>> entityList = new ArrayList<Map<String, Object>>();
	    	long t1 = System.currentTimeMillis();
	    	
      while (rs.next()) {
        if (i == importer.getAutoCommitSize()) {
          long t2 = System.currentTimeMillis();
          log.info("Time taken to Read " + i + " documents from SQL : "
              + (t2 - t1) + " ms");
          importer.getWriter().writeToNoSQL(entityList);
          entityList = new ArrayList<Map<String,Object>>();
          i = 0;
          t1 = System.currentTimeMillis();
        }
        params = new HashMap<String,String>();
        entityList.add(getFields(processor.toMap(rs), rs, rootEntity, null, null));
        i++;
      }
	    	importer.getWriter().writeToNoSQL(entityList);
	    }
	    
	    conn.close();
	    subConnection.close();
	}
	

	public Map<String, Object>  getFields(Map<String, Object> firstRow, ResultSet rs,  Entity entity, Map<String, Object> entityMap, Map<String, Object> rootEntityMap) throws SQLException {
		
			entityMap = new HashMap<String, Object>();
			
			if (entity.allAttributes.get(MULTI_VALUED) != null && entity.allAttributes.get(MULTI_VALUED).equalsIgnoreCase("true")) {
                getMultiValuedEntity(rs, entity, rootEntityMap);
			} else if(firstRow != null) {
                getSingleValuedEntity(firstRow, rs, entity, entityMap);
			}

		
    if (entity.entities != null) {
      Entity subEntity = null;
      String query = "", aparam = "";
      for (Iterator<Entity> iterator = entity.entities.iterator(); iterator.hasNext();) {
        subEntity = (Entity) iterator.next();
        subLevel = subConnection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        query = subEntity.allAttributes.get("query");
        
        m = p.matcher(query);
        aparam = "";
        try {
          log.info("Parameter Map is: " + params);
          while (m.find()) {
            aparam = query.substring(m.start() + 2, m.end() - 1);
            query = query.replaceAll("(\\$\\{" + aparam + "\\})", Matcher
                .quoteReplacement(StringEscapeUtils.escapeSql(params
                    .get(aparam))));
            m = p.matcher(query);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        resultSet = subLevel.executeQuery(query);
        if (resultSet.next()) {
          subEntityData = getFields(processor.toMap(resultSet), resultSet, subEntity, null, entityMap);
          if (subEntityData.size() > 0) entityMap.put(subEntity.name,
              subEntityData);
        }
        resultSet.close();
        subLevel.close();
      }
    }
		return entityMap;
	}

    private void getMultiValuedEntity(ResultSet rs, Entity entity, Map<String, Object> rootEntityMap) throws SQLException {
        List<Object> fieldArray = new ArrayList<Object>();
        rs.beforeFirst();
        while(rs.next()) {
            if (entity.fields.size() > 1) {
                Map<String, Object> entityFieldsMap = new HashMap<String, Object>();
                for (Iterator<Field> iterator = entity.fields.iterator(); iterator.hasNext();) {
                    Field field = iterator.next();
                    FieldType fieldType = FieldType.valueOf(field.allAttributes.get("type").toUpperCase());
                    entityFieldsMap.put(field.name, convertFieldType(fieldType, rs.getObject(field.column)).get(0));
                }
                fieldArray.add(entityFieldsMap);
            } else if (entity.fields.size() == 1) {
                fieldArray.add(rs.getObject(entity.fields.get(0).column));
            }
        }
        rootEntityMap.put(entity.name, fieldArray);
    }

    private void getSingleValuedEntity(Map<String, Object> firstRow, ResultSet rs, Entity entity, Map<String, Object> entityMap) throws SQLException {
        int totalRows = rs.getMetaData().getColumnCount();

        for (int i = 0; i < totalRows; i++) {
            extractFieldValue(firstRow, entity, entityMap, rs.getMetaData().getColumnLabel(i + 1),
                    rs.getMetaData().getColumnLabel(i + 1), null);
        }

        for (Iterator<Field> iterator = entity.fields.iterator(); iterator.hasNext();) {
            Field field = iterator.next();
            FieldType fieldType = FieldType.valueOf(field.allAttributes.get("type").toUpperCase());

            if (firstRow.get(field.column) != null) {
                entityMap.remove(field.column);
                extractFieldValue(firstRow, entity, entityMap, field.name, field.column, fieldType);
            } else if (field.defaultValue != null) {
                entityMap.put(field.name, field.defaultValue);
            }

        }
    }

    private void extractFieldValue(Map<String, Object> firstRow, Entity entity, Map<String, Object> entityMap, String fieldName, String fieldColumn, FieldType fieldType) {
        if (entity.pk != null && entity.pk.equals(fieldName)) {
             if (importer.getDataStoreType().equals(DataStoreType.MONGO) || importer.getDataStoreType().equals(DataStoreType.COUCH)) {
               entityMap.put("_id", convertFieldType(FieldType.STRING, firstRow.get(fieldColumn)).get(0));
             }
        } else if (fieldType != null) {
            entityMap.put(fieldName, convertFieldType(fieldType, firstRow.get(fieldColumn)).get(0));
        } else {
            entityMap.put(fieldName, firstRow.get(fieldColumn));
        }

        if (firstRow.get(fieldColumn) != null)
        	params.put(entity.name + "." + fieldName, firstRow.get(fieldColumn).toString());
        else
        	params.put(entity.name + "." + fieldName, "");
    }


    public List<Object> convertFieldType(FieldType fieldType, Object object) {
		List<Object> temp = new ArrayList<Object>(1);
		if (fieldType.equals(FieldType.DATE)) {
			temp.add( FieldTypeParser.dateToString(object));
		} else if (fieldType.equals(FieldType.STRING)) {
			temp.add( FieldTypeParser.getString(object));
		} else if (fieldType.equals(FieldType.INTEGER)) {
			temp.add( FieldTypeParser.getInt(object));
		} else if (fieldType.equals(FieldType.DOUBLE)) {
			temp.add( FieldTypeParser.getDouble(object));
		} else if (fieldType.equals(FieldType.LONG)) {
			temp.add( FieldTypeParser.getLong(object));
		} else if (fieldType.equals(FieldType.BOOLEAN)) {
			temp.add( FieldTypeParser.getBoolean(object));
		} else {
			temp.add(null);
		}
		return temp;
	}


	public static final String NAME = "name";
	  
	public static final String MULTI_VALUED = "multiValued";
	  
	public static final String DRIVER = "driver";
 
}


