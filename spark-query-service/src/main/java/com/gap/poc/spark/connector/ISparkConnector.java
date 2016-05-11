package com.gap.poc.spark.connector;

import com.gap.poc.spark.exception.SparkPocServiceException;

public interface ISparkConnector {

	String getSparkSessionId() throws SparkPocServiceException;

}
