
package com.gap.poc.spark.connector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gap.poc.spark.exception.SparkPocServiceException;

/**
 * Implementation of ISparkConnector interface to return the session
 * 
 * @author NIS1657-mbp
 *
 */
@Service
public class SparkConnectorImpl implements ISparkConnector {

	@Autowired
	private SparkHelper sparkHelper;

	@Override
	public String getSparkSessionId() throws SparkPocServiceException {
		return this.sparkHelper.getSparkSession();
	}

}
