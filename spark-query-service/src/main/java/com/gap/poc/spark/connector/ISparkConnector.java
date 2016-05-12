package com.gap.poc.spark.connector;

import com.gap.poc.spark.exception.SparkPocServiceException;

/**
 * This API exposes method to get spark session
 * 
 * @author NIS1657-mbp
 *
 */
public interface ISparkConnector {

	String getSparkSessionId() throws SparkPocServiceException;

}
