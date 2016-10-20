package com.example;

import com.amazonaws.services.sqs.model.CreateQueueRequest;

// creates an InMemoryQueue instance
public class InMemoryQueueFactory implements QueueFactory{

	@Override
	public synchronized MessageQueue create(CreateQueueRequest createQueueRequest) {
		MessageQueue queue = new InMemoryQueue(createQueueRequest.getQueueName(), new QueueAttributeValidatorImpl());
		queue.setAttributes(createQueueRequest.getAttributes());
		
		return queue;
	}

}
