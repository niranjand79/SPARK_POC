package com.gap.poc.spark.commons;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class Utilities {

	public JSONObject handleBadRequest(String message, Exception e) {
		JSONObject errorObject = new JSONObject();
		errorObject.put("timestamp", System.currentTimeMillis());
		errorObject.put("error", e.getMessage());
		errorObject.put("message", message);
		return errorObject;
	}

}
