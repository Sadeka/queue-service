package com.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class QueueAttributeValidatorImpl implements QueueAttributeValidator {
	private Map<String, AttributeValidator> attributeValidatorMap;
	private final static String visibilityTimeoutAttribute = "VisibilityTimeout";
	private final static Logger logger = Logger.getLogger(QueueAttributeValidatorImpl.class);
	
	public QueueAttributeValidatorImpl() {
		attributeValidatorMap = new HashMap<>();
		populateAttributes();
	}

	// In real world, these values may be populated from a config file.
	private void populateAttributes() {
		attributeValidatorMap.put(visibilityTimeoutAttribute, new VisibilityTimeoutValidator());
	}

	@Override
	public boolean validateAttributeName(String name) {
		return attributeValidatorMap.containsKey(name);
	}

	@Override
	public boolean validateAttributeValue(String name, String value) {
		if(validateAttributeName(name))
			return attributeValidatorMap.get(name).validate(value);
		 
		logger.error(String.format("Invalid attribute %s", name));
		return false;
	}

	@Override
	public String getDefaultValue(String name) {
		if(validateAttributeName(name))
			return attributeValidatorMap.get(name).getDefault();
		else 
			logger.error(String.format("Invalid attribute %s", name));
		
		return null;
	}

	@Override
	public Map<String, String> getDefaultAttributeValues() {
		Map<String, String> defaultAttributeValues = new HashMap<>();
		
		for(Map.Entry<String, AttributeValidator> entry: this.attributeValidatorMap.entrySet())
			defaultAttributeValues.put(entry.getKey(), entry.getValue().getDefault());
			
		return defaultAttributeValues;
	}

}
