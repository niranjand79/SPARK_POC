
package com.gap.poc.spark.connector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONException;
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
		HttpGet getRequest = new HttpGet(Constants.SPARK_SESSION_URL);
		HttpResponse getResponse = null;
		try {
			getResponse = httpClient.execute(getRequest);
		} catch (ClientProtocolException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			JSONObject responseObj = new JSONObject(IOUtils.toString(getResponse.getEntity().getContent()));
			if (responseObj.getInt("sessionsCount") != 0) {
				JSONArray sessionsArray = responseObj.getJSONArray("sessions");
				JSONObject sessionObject = sessionsArray.getJSONObject(0);
				return sessionObject.getString("id");
			}
		} catch (JSONException e1) {
			e1.printStackTrace();
		} catch (UnsupportedOperationException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
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
			JSONObject jsonObject = new JSONObject(IOUtils.toString(response.getEntity().getContent()));
			JSONObject session = jsonObject.getJSONObject("session");
			return session.getString("id");
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
			JSONObject jsonObject = new JSONObject(Constants.SPARK_CONTEXT_REQUEST);
			System.out.println(jsonObject.toString());
			entity = new StringEntity(jsonObject.toString());
		} catch (UnsupportedEncodingException e) {
			// log.error("Error while creating request context", e);
			throw new SparkPocServiceException("Error while creating request context", e);
		}
		HttpPost request = new HttpPost(Constants.SPARK_CONTEXT_URL);
		request.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		request.setEntity(entity);
		HttpResponse response = null;
		try {
			response = httpClient.execute(request);
			System.out.println(IOUtils.toString(response.getEntity().getContent()));
		} catch (IOException e) {
			throw new SparkPocServiceException("Error while executing request for creating the context", e);
		}
		if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201)
			throw new SparkPocServiceException("Error while creating the spark context");
	}

	/**
	 * This methods deletes the spark context
	 * 
	 * @throws SparkPocServiceException
	 */
	public void deleteSparkSession(final String sessionId) throws SparkPocServiceException {
		StringBuilder uriBuilder = new StringBuilder(Constants.SPARK_SESSION_URL);
		uriBuilder.append(sessionId);
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpDelete request = new HttpDelete(uriBuilder.toString());
		request.setHeader("Content-type", Constants.CONTENT_TYPE_JSON);
		HttpResponse response = null;
		try {
			response = httpClient.execute(request);
		} catch (IOException e) {
			throw new SparkPocServiceException("Error while executing request for deleting the session", e);
		}
		if (response.getStatusLine().getStatusCode() != 200)
			throw new SparkPocServiceException("Error while deleting the spark session");

	}

}
