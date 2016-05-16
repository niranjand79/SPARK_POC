package com.gap.poc.spark.commons;

import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * This class contains common methods used across the modules
 * 
 * @author NIS1657-mbp
 *
 */
@Component
public class Utilities {

	public JSONObject handleBadRequest(String message, Exception e) {
		JSONObject errorObject = new JSONObject();
		errorObject.put("timestamp", System.currentTimeMillis());
		errorObject.put("error", e.getMessage());
		errorObject.put("message", message);
		return errorObject;
	}

	/**
	 * This method build the SQL query based on the arguments passed in GET
	 * request
	 * 
	 * @param fileName
	 * @param columnNames
	 * @param filterCondition
	 * @return
	 */
	public String SQLBuilder(final String columnNames, final String filterCondition) {
		StringBuilder sqlBuilder = new StringBuilder(Constants.SQL_QUERY_SELECT);
		if (!StringUtils.isEmpty(filterCondition)) {
			String sqlCondition = filterCondition.replace(",", "\\\\' AND ").replace(":", "=\\\\'");
			sqlBuilder.append(" WHERE").append(" ").append(sqlCondition).append("\\\\'");
		}
		if (StringUtils.isEmpty(columnNames)) {
			return sqlBuilder.toString().replace("<columnNames>", "*");
		} else {
			return sqlBuilder.toString().replace("<columnNames>", columnNames);
		}
	}

}
