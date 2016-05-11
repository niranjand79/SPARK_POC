/**
 * This class contains method to create return spark session.
 * @date 05-10-2016
 */
package com.gap.poc.spark.connector;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gap.poc.spark.exception.SparkPocServiceException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SparkConnectorImpl implements ISparkConnector {

	@Autowired
	private SparkHelper sparkHelper;

	@Override
	public String getSparkSessionId() throws SparkPocServiceException {
		JSONObject jsonObject = new JSONObject(this.sparkHelper.getSparkSession());
		JSONObject session = jsonObject.getJSONObject("session");
		return session.getString("id");
	}

}
