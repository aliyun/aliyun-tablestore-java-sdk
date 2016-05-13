package com.aliyun.openservices.ots.utils;

import java.util.List;

/**
 * Utils for common coding.
 * 
 */
public class CodingUtils {
	private static ResourceManager rm = ResourceManager
			.getInstance(ServiceConstants.RESOURCE_NAME_COMMON);

	public static void assertParameterNotNull(Object param, String paramName) {
		if (param == null) {
			throw new NullPointerException(rm.getFormattedString(
					"ParameterIsNull", paramName));
		}
	}

	public static void assertStringNotNullOrEmpty(String param, String paramName) {
		assertParameterNotNull(param, paramName);
		if (param.length() == 0) {
			throw new IllegalArgumentException(rm.getFormattedString(
					"ParameterStringIsEmpty", paramName));
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static void assertListNotNullOrEmpty(List param, String paramName){
		assertParameterNotNull(param, paramName);
		if (param.size() == 0) {
			throw new IllegalArgumentException(rm.getFormattedString(
					"ParameterListIsEmpty", paramName));
		}
	}
	

	public static boolean isNullOrEmpty(String value) {
		return value == null || value.length() == 0;
	}
}
