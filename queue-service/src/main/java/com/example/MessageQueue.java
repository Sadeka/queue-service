package com.example;

import java.util.Map;

import com.amazonaws.services.sqs.model.Message;

public interface MessageQueue {
	String getName();
	
	void push(Message message);
	Message pull();
	boolean delete(String receiptHandle);
	
	// get available number of messages
	int getApproximateNumberOfMessages();
	int getNumberOfInflightMessages();
	
	// refreshes the queue state - i.e., puts the expired messages in the available queue
	void refreshQueue();
	boolean purge();
	
	void setAttributes(Map<String, String> attributes);
	Map<String, String> getAttributes();
	
	// cancels the timer for periodic refreshQueue()
	void releaseResources();
}
