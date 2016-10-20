package com.example;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;

@RunWith(MockitoJUnitRunner.class)
public class InMemoryQueueServiceTest {
	// this mockAppender is used for validating logged messages.
	@Mock
    private Appender mockAppender;
    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;

    @Before
    public void setUp() {
    	LogManager.getRootLogger().addAppender(mockAppender);
    }
    
    @After
    public void teardown() {
        LogManager.getRootLogger().removeAppender(mockAppender);
    }
    
	@Test
	public void testCreateSingleQueueWithNullRequest() {
		CreateQueueRequest createQueueRequest = null;
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = "Queue request is null. Failed to create a queue.";
		
		boolean createQueueStatus = inMemoryQueueService.createQueue(createQueueRequest);
		assertFalse(createQueueStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
		assertEquals(0, inMemoryQueueService.getQueueNames().size());
	}
	
	@Test
	public void testCreateSingleQueueWithValidName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		createMockMessageQueue(createQueueRequest, queueFactory);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
			
		boolean createQueueStatus = inMemoryQueueService.createQueue(createQueueRequest);
		
		assertTrue(createQueueStatus);
		assertTrue(inMemoryQueueService.getQueueNames().contains(qName));
	}
	
	@Test
	public void testCreateSingleQueueWithDuplicateName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		createMockMessageQueue(createQueueRequest, queueFactory);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name already exists: %s", qName);
		
		boolean firstCreateQueueStatus = inMemoryQueueService.createQueue(createQueueRequest);
		assertTrue(firstCreateQueueStatus);
		boolean secondCreateQueueStatus = inMemoryQueueService.createQueue(createQueueRequest);
		assertFalse(secondCreateQueueStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
		assertEquals(1, inMemoryQueueService.getQueueNames().size());
	}
	
	@Test
	public void testCreateSingleQueueWithEmptyName() {
		String qName = "";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory mockQueueFactory = mock(QueueFactory.class);
		QueueService inMemoryQueueService = new InMemoryQueueService(mockQueueFactory);
		String expectedError = "Queue name is null or empty. Failed to create a queue.";
		
		boolean createQueueStatus = inMemoryQueueService.createQueue(createQueueRequest);
		
		assertFalse(createQueueStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
		assertEquals(0, inMemoryQueueService.getQueueNames().size());
	}
	
	@Test
	public void testCreateSingleQueueWithNullName() {
		String qName = null;
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		createMockMessageQueue(createQueueRequest, queueFactory);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = "Queue name is null or empty. Failed to create a queue.";
		
		boolean createQueueStatus = inMemoryQueueService.createQueue(createQueueRequest);
		
		assertFalse(createQueueStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
		assertEquals(0, inMemoryQueueService.getQueueNames().size());
	}
	
	@Test
	public void testCreateMultipleQueueWithValidName() {
		String qName1 = "MyQueue1";
		CreateQueueRequest createQueueRequest1 = new CreateQueueRequest(qName1);
		String qName2 = "MyQueue2";
		CreateQueueRequest createQueueRequest2 = new CreateQueueRequest(qName2);
		QueueFactory queueFactory = createMockQueueFactory();
		createMockMessageQueue(createQueueRequest1, queueFactory);
		createMockMessageQueue(createQueueRequest2, queueFactory);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		
		boolean firstCreateQueueStatus = inMemoryQueueService.createQueue(createQueueRequest1);
		assertTrue(firstCreateQueueStatus);
		
		boolean secondCreateQueueStatus = inMemoryQueueService.createQueue(createQueueRequest2);
		assertTrue(secondCreateQueueStatus);
		assertEquals(2, inMemoryQueueService.getQueueNames().size());
		assertTrue(inMemoryQueueService.getQueueNames().contains(qName1));
		assertTrue(inMemoryQueueService.getQueueNames().contains(qName2));
	}
	
	@Test
	public void testDeleteQueueWithValidName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		createMockMessageQueue(createQueueRequest, queueFactory);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		
		inMemoryQueueService.createQueue(createQueueRequest);
		assertEquals(1, inMemoryQueueService.getQueueNames().size());
		
		boolean deleteQueueStatus = inMemoryQueueService.deleteQueue(qName);
		assertTrue(deleteQueueStatus);
		assertEquals(0, inMemoryQueueService.getQueueNames().size());
	}
	
	@Test
	public void testDeleteQueueWithInvalidName() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to delete the queue %s", invalidQName);
		
		boolean deleteQueueStatus = inMemoryQueueService.deleteQueue(invalidQName);
		assertFalse(deleteQueueStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPushNullMessageIntoValidQueueName() {
		String messageBody = null;
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		doNothing().when(queue1).push(any(Message.class));
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		String expectedError = String.format("Message body is null or empty. Failed to push message into queue %s",qName);
		
		String receivedMessageId = inMemoryQueueService.push(qName, messageBody);
		assertNull(receivedMessageId);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPushEmptyMessageIntoValidQueueName() {
		String messageBody = "";
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		doNothing().when(queue1).push(any(Message.class));
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		String expectedError = String.format("Message body is null or empty. Failed to push message into queue %s",qName);
		
		String receivedMessageId = inMemoryQueueService.push(qName, messageBody);
		assertNull(receivedMessageId);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPushNonEmptyMessageIntoValidQueueName() {
		String messageBody = "Hello queue";
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		doNothing().when(queue1).push(any(Message.class));
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		String receivedMessageId = inMemoryQueueService.push(qName, messageBody);
		assertEquals(36, receivedMessageId.length());
	}
	
	@Test
	public void testPushNullMessageIntoInvalidQueueName() {
		String messageBody = null;
		String qName = "NonExistentQ";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		String expectedError = String.format("Queue name not found. Failed to push message into %s", qName);
		
		String receivedMessageId = inMemoryQueueService.push(qName, messageBody);
		assertNull(receivedMessageId);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPushEmptyMessageIntoInvalidQueueName() {
		String messageBody = "";
		String qName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to push message into %s", qName);
		
		String receivedMessageId = inMemoryQueueService.push(qName, messageBody);
		assertNull(receivedMessageId);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPushNonEmptyMessageIntoInvalidQueueName() {
		String messageBody = "Hello queue";
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to push message into %s", invalidQName);
		
		String receivedMessageId = inMemoryQueueService.push(invalidQName, messageBody);
		assertNull(receivedMessageId);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPullMessageFromValidQueueName() {
		String messageBody = "Hello queue";
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		Message message = MessageCreator.createMessage(messageBody);
		message.setReceiptHandle(String.valueOf(UUID.randomUUID()));
		when(queue1.pull()).thenReturn(message);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		Message receivedMessage = inMemoryQueueService.pull(qName);
		assertEquals(message, receivedMessage);
	}
	
	@Test
	public void testPullMessageFromInvalidQueueName() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to pull message from %s", invalidQName);
		
		Message receivedMessage = inMemoryQueueService.pull(invalidQName);
		assertNull(receivedMessage);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testDeleteMessageFromValidQueueName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		String receiptHandle = String.valueOf(UUID.randomUUID());
		when(queue1.delete(receiptHandle)).thenReturn(true);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		boolean deleteMessageStatus = inMemoryQueueService.delete(qName, receiptHandle);
		assertTrue(deleteMessageStatus);
	}
	
	@Test
	public void testDeleteMessageFromInvalidQueueName() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		String receiptHandle = String.valueOf(UUID.randomUUID());
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to delete message from %s", invalidQName);
		
		boolean deleteMessageStatus = inMemoryQueueService.delete(invalidQName, receiptHandle);
		assertFalse(deleteMessageStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testPurgeValidQueueName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		when(queue1.purge()).thenReturn(true);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		boolean purgeQueueStatus = inMemoryQueueService.purgeQueue(qName);
		assertTrue(purgeQueueStatus);
	}
	
	@Test
	public void testPurgeInvalidQueueName() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to purge queue %s", invalidQName);
		
		boolean purgeQueueStatus = inMemoryQueueService.purgeQueue(invalidQName);
		assertFalse(purgeQueueStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testSetAttributesValidQueueName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		Map<String, String> attributes = new HashMap<>();
		String attributeName = "VisibilityTimeout";
		String attributeValue = "60";
		attributes.put(attributeName, attributeValue);
		doNothing().when(queue1).setAttributes(attributes);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		inMemoryQueueService.setQueueAttributes(qName, attributes);
		verify(queue1, times(1)).setAttributes(attributes);
	}
	
	@Test
	public void testSetAttributesInvalidQueue() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		Map<String, String> attributes = new HashMap<>();
		String attributeName = "VisibilityTimeout";
		String attributeValue = "60";
		attributes.put(attributeName, attributeValue);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to set attributes of %s", invalidQName);
		
		inMemoryQueueService.setQueueAttributes(invalidQName, attributes);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testGetAttributesValidQueueName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		Map<String, String> attributes = new HashMap<>();
		String attributeName = "VisibilityTimeout";
		String attributeValue = "60";
		attributes.put(attributeName, attributeValue);
		when(queue1.getAttributes()).thenReturn(attributes);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		Map<String, String> receivedAttributes = inMemoryQueueService.getQueueAttributes(qName);
		assertEquals(attributes, receivedAttributes);
	}
	
	@Test
	public void testGetAttributesInvalidQueueName() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to get attributes of %s", invalidQName);
		
		Map<String, String> receivedAttributes = inMemoryQueueService.getQueueAttributes(invalidQName);
		assertNull(receivedAttributes);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testGetApproximateNumberOfMessagesForInvalidQueueName() {
		String invalidQName = "NonExistentQ";
		QueueFactory queueFactory = createMockQueueFactory();
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		String expectedError = String.format("Queue name not found. Failed to count messages in %s", invalidQName);
		
		int receivedCount = inMemoryQueueService.getApproximateNumberOfMessages(invalidQName);
		assertEquals(-1, receivedCount);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testGetApproximateNumberOfMessagesForValidQueueName() {
		String qName = "MyQueue1";
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
		QueueFactory queueFactory = createMockQueueFactory();
		MessageQueue queue1 = createMockMessageQueue(createQueueRequest, queueFactory);
		int mockedCount = 3;
		when(queue1.getApproximateNumberOfMessages()).thenReturn(mockedCount);
		QueueService inMemoryQueueService = new InMemoryQueueService(queueFactory);
		inMemoryQueueService.createQueue(createQueueRequest);
		
		int receivedCount = inMemoryQueueService.getApproximateNumberOfMessages(qName);
		assertEquals(mockedCount, receivedCount);
	}
	
	// testGetQueueNames - empty service, nonempty service
	
	private QueueFactory createMockQueueFactory() {
		QueueFactory mockQueueFactory = mock(QueueFactory.class);
		return mockQueueFactory;
	}
	
	private MessageQueue createMockMessageQueue(CreateQueueRequest createQueueRequest, QueueFactory mockQueueFactory) {
		MessageQueue mockMessageQueue = mock(MessageQueue.class);
		when(mockQueueFactory.create(createQueueRequest)).thenReturn(mockMessageQueue);
		return mockMessageQueue;
	}
	
	private void verifyLoggedMessage(String expectedMessage, Level expectedLevel) {
		verify(mockAppender).doAppend(captorLoggingEvent.capture());
		LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(expectedLevel));
        assertThat(loggingEvent.getRenderedMessage(), is(expectedMessage));
	}
}
