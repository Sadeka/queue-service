package com.example;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.sqs.model.CreateQueueRequest;

// this test is not very interesting; just makes sure that the right instance type is returned
@RunWith(MockitoJUnitRunner.class)
public class InMemoryQueueFactoryTest {
	
	QueueFactory queueFactory;
	
	@Before
	public void setUp() {
		queueFactory = new InMemoryQueueFactory();
	}
	
	@Test
	public void testCreateInMemoryQueue() {
		String qName = "MyQueue";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		
		MessageQueue result = queueFactory.create(createQueueRequest);
		assertTrue(result instanceof MessageQueue);
		assertTrue(result.getName().equals(qName));
	}

}
