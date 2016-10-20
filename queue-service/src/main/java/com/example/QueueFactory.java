package com.example;

import com.amazonaws.services.sqs.model.CreateQueueRequest;

// Factory for creating queue instances (Factory method pattern)
public interface QueueFactory {
	MessageQueue create(CreateQueueRequest createQueueRequest);
}
