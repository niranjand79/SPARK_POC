package com.poc.spark.commons;

/**
 * This class contains constants used across the module.
 * 
 * @author NIS1657-mbp
 *
 */
public class Constants {

	public static final String SPARK_CONTEXT_URL = "http://192.168.99.100:3000/v1/contexts/mycontext";
	
	public static final String SPARK_CONTEXT_REQUEST = "{\"config\":{\"createSQLContext\": true,\"properties\":"
			+ " {\"spark.driver.cores\": \"1\",\"spark.driver.memory\": \"1g\","
			+ "\"spark.executor.memory\": \"2g\",\"spark.shuffle.sort.bypassMergeThreshold\": \"8\"},"
			+ "\"packages\": [\"com.databricks:spark-csv_2.10:1.3.0\"]}}";
	public static final String CONTENT_TYPE_JSON = "application/json";
	public static final String SPARK_SESSION_URL = "http://192.168.99.100:3000/v1/contexts/mycontext/sessions/";
	public static final String SPARK_LOAD_CSV_BASE_URL = "http://192.168.99.100:3000/v1/contexts/mycontext/sessions/";
	public static final String QUERY_JSON = "{"
			+ "\"code\": [\"var people = sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('<fileName>.csv'))\","
			+ "\"people.registerTempTable('DUMMYTABLE')\",\"var result = sqlContext.sql('<sqlQuery>')\"],\"return\": \"result\"}";
	public static final String SQL_QUERY_SELECT = "SELECT <columnNames> FROM DUMMYTABLE";
}
