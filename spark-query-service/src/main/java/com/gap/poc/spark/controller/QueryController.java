
package com.gap.poc.spark.controller;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.gap.poc.spark.commons.Utilities;
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
	@Autowired
	private Utilities utilities;

	@RequestMapping(value = "/query", method = RequestMethod.POST)
	public String loadCsv(@RequestBody String requestJson) throws SparkPocServiceException {
		JSONObject jsonObject = null;
		try {
			jsonObject = new JSONObject(requestJson);
		} catch (JSONException e) {
			return this.utilities.handleBadRequest("Request JSON is invalid", e).toString();
		}
		String sessionId = this.sparkConnector.getSparkSessionId();

		return this.csvOperations.getData(jsonObject.getString("fileName"), jsonObject.getString("query"), sessionId);
	}

}
