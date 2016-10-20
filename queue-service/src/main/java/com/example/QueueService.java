package com.example;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;

public interface QueueService {

	// This interface may have different implementations, such as InMemory Queue Service, File based 
	// Queue Service and Adapter for Cloud based Queue Service, e.g., SQS
	
	boolean createQueue(CreateQueueRequest createQueueRequest);
	
	boolean deleteQueue(String qName);
	
	List<String> getQueueNames();
	
	void setQueueAttributes(String qName, Map<String, String> attributes);
	
	Map<String, String> getQueueAttributes(String qName);
	
	// gets the number of available messages in the queue
	int getApproximateNumberOfMessages(String qName);
	
	// clears the specified queue
	boolean purgeQueue(String qName);
	
    // pushes a message onto a queue.
	String push(String qName, String messageBody);
	
    // retrieves a single message from a queue.
	Message pull(String qName);
	
	// deletes a message from the queue that was received by pull().
	boolean delete(String qName, String receiptHandle);
}
