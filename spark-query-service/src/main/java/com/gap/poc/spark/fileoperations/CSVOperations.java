package com.gap.poc.spark.fileoperations;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gap.poc.spark.commons.Constants;
import com.gap.poc.spark.commons.Utilities;
import com.gap.poc.spark.exception.SparkPocServiceException;

/**
 * This class contains methods for CSV file operations.
 * 
 * @author NIS1657-mbp
 *
 */
@Component
public class CSVOperations {

	@Autowired
	private Utilities utilities;

	public String getData(final String sessionId, final String tableName, final String columnNames,
			final String filterCondition) {
		try {
			loadCSV(sessionId, tableName);
			return executeQuery(sessionId, tableName, columnNames, filterCondition);
		} catch (SparkPocServiceException e) {
			return this.utilities.handleBadRequest("Error while retrieving the data", e).toString();
		}

	}

	public String executeQuery(final String sessionId, final String fileName, final String columnNames,
			final String filterCondition) throws SparkPocServiceException {

		// create a POST request to apply filer
		String sqlQuery = this.utilities.SQLBuilder(columnNames, filterCondition);
		JSONObject queryJson = new JSONObject(
				Constants.QUERY_JSON.replace("<sqlQuery>", sqlQuery).replace("<fileName>", fileName));
		// queryJson.put(Constants.QUERY_JSON);
		StringEntity filterEntity;
		try {
			filterEntity = new StringEntity(queryJson.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while forming request JSON to filer csv file " + fileName, e);
		}
		StringBuilder uriBuilder = new StringBuilder(Constants.SPARK_LOAD_CSV_BASE_URL);
		uriBuilder.append(sessionId);
		uriBuilder.append("/statements");
		HttpPost filterRequest = new HttpPost(uriBuilder.toString());
		filterRequest.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		filterRequest.setEntity(filterEntity);
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpResponse filterResponse = null;
		try {
			filterResponse = httpClient.execute(filterRequest);
		} catch (IOException e) {
			// log.error(
			// "Error while executing request for creating the context", e);
			throw new SparkPocServiceException("Error while executing request to query the table", e);
		}
		try {
			if (filterResponse.getStatusLine().getStatusCode() != 200) {
				throw new SparkPocServiceException("Error while filtering data from csv file");
			}
			return IOUtils.toString(filterResponse.getEntity().getContent());
		} catch (IOException e) {
			// log.error("Error while parsing response from the context", e);
			throw new SparkPocServiceException("Error while parsing response from the filter request", e);
		}

	}

	/**
	 * This methods loads the csv file passed as an input using already acquired
	 * session id and applies the filter criteria
	 * 
	 * @param fileName
	 *            data file
	 * @param filter
	 *            query parameters
	 * @param sessionId
	 *            session id
	 * @return filtered output data
	 * @throws SparkPocServiceException
	 */
	private void loadCSV(String sessionId, final String tableName) throws SparkPocServiceException {
		// create json for POST request
		JSONObject jsonObject = new JSONObject();
		JSONArray jsonArray = new JSONArray();
		jsonArray.put(0,
				"var cars = sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('"
						+ tableName + ".csv'))");
		jsonObject.put("return", "cars.schema()");
		jsonObject.put("code", jsonArray);
		StringEntity entity;
		try {
			entity = new StringEntity(jsonObject.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while forming request JSON to load csv file " + tableName, e);
		}
		// create post request to load the file
		StringBuilder uriBuilder = new StringBuilder(Constants.SPARK_LOAD_CSV_BASE_URL);
		uriBuilder.append(sessionId);
		uriBuilder.append("/statements");
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost request = new HttpPost(uriBuilder.toString());
		request.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		request.setEntity(entity);
		HttpResponse response = null;
		try {
			response = httpClient.execute(request);
		} catch (IOException e) {
			// log.error(
			// "Error while executing request for creating the context", e);
			throw new SparkPocServiceException("Error while loading csv file " + tableName, e);
		}
		if (response.getStatusLine().getStatusCode() != 200) {
			throw new SparkPocServiceException("Error while loading csv file from spark server");
		}

	}

}
