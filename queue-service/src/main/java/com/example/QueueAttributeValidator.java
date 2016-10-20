package com.example;

import java.util.Map;

public interface QueueAttributeValidator {
	boolean validateAttributeName(String name);
	boolean validateAttributeValue(String name, String value);
	String getDefaultValue(String name);
	Map<String, String> getDefaultAttributeValues();
}