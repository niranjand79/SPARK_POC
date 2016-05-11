
package com.gap.poc.spark.controller;

import java.net.URISyntaxException;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.gap.poc.spark.connector.ISparkConnector;
import com.gap.poc.spark.exception.SparkPocServiceException;
import com.gap.poc.spark.fileoperations.CSVOperations;

/**
 * This controller class contains methods exposed as REST service
 * 
 * @date 05-10-2016
 *
 */
@RestController
@EnableAutoConfiguration
public class QueryController {

	@Autowired
	private ISparkConnector sparkConnector;
	@Autowired
	private CSVOperations csvOperations;

	@RequestMapping(value = "/query", method = RequestMethod.POST)
	public String loadCsv(@RequestBody String requestJson) throws SparkPocServiceException {

		JSONObject jsonObject = new JSONObject(requestJson);
		String sessionId = this.sparkConnector.getSparkSessionId();
		return sessionId;
		// try {
		// return this.csvOperations.loadCSV(jsonObject.getString("fileName"),
		// sessionId);
		// } catch (JSONException e) {
		// return "ERROR";
		// } catch (URISyntaxException e) {
		// return "ERROR";
		// }
	}

}
