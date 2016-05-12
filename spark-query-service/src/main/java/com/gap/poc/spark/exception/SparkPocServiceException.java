package com.gap.poc.spark.exception;

/**
 * This is custom exception class designed for current module
 * 
 * @author NIS1657-mbp
 *
 */
public class SparkPocServiceException extends Exception {

	private static final long serialVersionUID = 1L;

	public SparkPocServiceException(String message) {

		super(message);
	}

	public SparkPocServiceException(String message, Exception e) {
		super(message, e);
	}

	public SparkPocServiceException(Exception e) {
		super(e);
	}

}
