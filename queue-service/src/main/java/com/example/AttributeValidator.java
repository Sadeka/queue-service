package com.example;

// This interface validates a single attribute and returns the default value of that attribute
public interface AttributeValidator {
	boolean validate(String value);
	String getDefault();
}
