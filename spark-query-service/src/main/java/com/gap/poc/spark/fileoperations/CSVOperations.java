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

	public String getData(String fileName, String filter, String sessionId) {

		try {
			return loadCSV(fileName, filter, sessionId);
		} catch (SparkPocServiceException e) {
			return this.utilities.handleBadRequest("Unable to load csv file", e).toString();
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
	public String loadCSV(String fileName, String filter, String sessionId) throws SparkPocServiceException {
		// create json for POST request
		JSONObject jsonObject = new JSONObject();
		JSONArray jsonArray = new JSONArray();
		jsonArray.put(0,
				"var cars = sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('"
						+ fileName + "'))");
		jsonObject.put("return", "cars.schema()");
		jsonObject.put("code", jsonArray);
		StringEntity entity;
		try {
			entity = new StringEntity(jsonObject.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while forming request JSON to load csv file " + fileName, e);
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
			throw new SparkPocServiceException("Error while loading csv file " + fileName, e);
		}
		if (response.getStatusLine().getStatusCode() != 200) {
			throw new SparkPocServiceException("Error while loading csv file from spark server");
		}

		// create a POST request to apply filer
		JSONObject filterJson = new JSONObject();
		filterJson.put("return", filter);
		StringEntity filterEntity;
		try {
			filterEntity = new StringEntity(filterJson.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while forming request JSON to filer csv file " + fileName, e);
		}
		HttpPost filterRequest = new HttpPost(uriBuilder.toString());
		filterRequest.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		filterRequest.setEntity(filterEntity);
		HttpResponse filterResponse = null;
		try {
			filterResponse = httpClient.execute(filterRequest);
		} catch (IOException e) {
			// log.error(
			// "Error while executing request for creating the context", e);
			throw new SparkPocServiceException("Error while applying filter " + filter, e);
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
}
