package com.example;

import org.apache.log4j.Logger;

public class VisibilityTimeoutValidator implements AttributeValidator {
	private final int minValue = 0;
	private final int maxValue = 43200;
	private final int defaultValue = 30;
	private final static Logger logger = Logger.getLogger(VisibilityTimeoutValidator.class);
	
	@Override
	public boolean validate(String value) {
		boolean isValid = false;
		
		try{
			int parsedValue = Integer.valueOf(value);
			
			if(parsedValue >= minValue && parsedValue <= maxValue)
				isValid = true;
			else 
				logger.error(String.format("Invalid value - lies outside the acceptable range %s", value));
			
		}catch(NumberFormatException e) {
			logger.error(String.format("Exception while parsing value %s", value));
			e.printStackTrace();
		}
		
		return isValid;
	}

	@Override
	public String getDefault() {
		return String.valueOf(defaultValue);
	}

}
