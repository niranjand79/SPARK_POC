
package com.poc.spark.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.poc.spark.connector.ISparkConnector;
import com.poc.spark.exception.SparkPocServiceException;
import com.poc.spark.fileoperations.CSVOperations;

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

	@RequestMapping(value = "/query", method = RequestMethod.GET, produces = "application/json")

	public String loadCsv(@RequestParam(value = "tableName", required = true) String tableName,
			@RequestParam(value = "columnNames", required = false) String columnNames,
			@RequestParam(value = "filter", required = false) String filterCondition) throws SparkPocServiceException {
		String sessionId = this.sparkConnector.getSparkSessionId();
		return this.csvOperations.getData(sessionId, tableName, columnNames, filterCondition);
	}

}
