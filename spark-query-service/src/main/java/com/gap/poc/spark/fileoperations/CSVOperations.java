package com.gap.poc.spark.fileoperations;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import com.gap.poc.spark.constants.Constants;
import com.gap.poc.spark.exception.SparkPocServiceException;

@Component
public class CSVOperations {

	public String loadCSV(String fileName, String sessionId) throws SparkPocServiceException, URISyntaxException {
		// create json for POST request
		JSONObject jsonObject = new JSONObject();
		JSONArray jsonArray = new JSONArray();
		jsonArray.put(0,
				"var people = sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('"
						+ fileName + "'))");
		jsonObject.put("return", "people.schema()");
		jsonObject.put("code", jsonArray);
		StringEntity entity;
		try {
			entity = new StringEntity(jsonObject.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while forming request JSON to load csv file " + fileName, e);
		}
		// create post request
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
		try {
			return IOUtils.toString(response.getEntity().getContent());
		} catch (IOException e) {
			// log.error("Error while parsing response from the context", e);
			throw new SparkPocServiceException("Error while parsing response from the context", e);
		}

	}

}
