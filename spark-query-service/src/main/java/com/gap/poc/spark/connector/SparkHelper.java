
package com.gap.poc.spark.connector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import com.gap.poc.spark.commons.Constants;
import com.gap.poc.spark.exception.SparkPocServiceException;

/**
 * This helper class contains methods to acquire spark resources
 * 
 * @date 05-10-2016
 *
 */
@Component
public class SparkHelper {
	/**
	 * This method creates the spark session from the spark context and return
	 * the session id
	 * 
	 * @return spark session id
	 * @throws SparkPocServiceException
	 *             custom service exception
	 */
	public String getSparkSession() throws SparkPocServiceException {
		getSparkContext();
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost request = new HttpPost(Constants.SPARK_SESSION_URL);
		request.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		HttpResponse response = null;
		try {
			response = httpClient.execute(request);
		} catch (IOException e) {
			// log.error(
			// "Error while executing request for getting the session", e);
			throw new SparkPocServiceException("Error while executing request for getting the session", e);
		}
		try {
			return IOUtils.toString(response.getEntity().getContent());
		} catch (IOException e) {
			// log.error("Error while parsing response from the session", e);
			throw new SparkPocServiceException("Error while parsing response from the session", e);
		}
	}

	/**
	 * This method return creates the spark context
	 * 
	 * @throws SparkPocServiceException
	 *             custom service exception
	 */
	private void getSparkContext() throws SparkPocServiceException {

		HttpClient httpClient = HttpClientBuilder.create().build();
		StringEntity entity;
		try {
			JSONObject jsonObject = new JSONObject(new JSONObject(Constants.SPARK_CONTEXT_REQUEST));
			entity = new StringEntity(jsonObject.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while creating request context", e);
		}
		HttpPost request = new HttpPost(Constants.SPARK_VM_URL);
		request.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		request.setEntity(entity);
		HttpResponse response = null;
		try {
			response = httpClient.execute(request);
		} catch (IOException e) {
			// log.error(
			// "Error while executing request for creating the context", e);
			throw new SparkPocServiceException("Error while executing request for creating the context", e);
		}
		// return IOUtils.toString(response.getEntity().getContent());
		if (response.getStatusLine().getStatusCode() != 200)
			throw new SparkPocServiceException("Error while creating the spark context");
	}

}
