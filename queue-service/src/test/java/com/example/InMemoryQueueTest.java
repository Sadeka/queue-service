package com.example;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
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

import com.amazonaws.services.sqs.model.Message;

@RunWith(MockitoJUnitRunner.class)
public class InMemoryQueueTest {
	String validAttributeName = "VisibilityTimeout";
	String validAttributeMinValue = "0";
	String validAttributeDefaultValue = "30";
	String validAttributeInvalidValue = "80000";
	String invalidAttributeName = "GeneralTimeout";
	Map<String, String> validAttributeValues;
	
	String qName = "MyQueue";
	MessageQueue queue;
	
	QueueAttributeValidator validator;
	
	@Mock
    private Appender mockAppender;
    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;
    
    @After
    public void teardown() {
        LogManager.getRootLogger().removeAppender(mockAppender);
    }
    
	@Before
	public void setUp() {
		validAttributeValues = new HashMap<>();
		validAttributeValues.put(validAttributeName, validAttributeDefaultValue);
		validator = mock(QueueAttributeValidator.class);
		when(validator.getDefaultValue(validAttributeName)).thenReturn(validAttributeDefaultValue);
		when(validator.getDefaultAttributeValues()).thenReturn(validAttributeValues);
		when(validator.validateAttributeName(validAttributeName)).thenReturn(true);
		when(validator.validateAttributeValue(validAttributeName, validAttributeMinValue)).thenReturn(true);
		when(validator.validateAttributeValue(validAttributeName, validAttributeInvalidValue)).thenReturn(false);
		
		queue = new InMemoryQueue(qName, validator);
		LogManager.getRootLogger().addAppender(mockAppender);
	}
	
	
	@Test
    public void testPushMessage() {
		String msgBody = "Hello Queue!";
		Message message = MessageCreator.createMessage(msgBody);
		
		queue.push(message);
		
		assertEquals(36, message.getMessageId().length());
		assertEquals(1, queue.getApproximateNumberOfMessages());
	}
	
	@Test
    public void testPullMessageForNonEmptyQueue() {
		String msgBody = "Hello Queue!";
		Message message = MessageCreator.createMessage(msgBody);
		
		queue.push(message);
		Message actualMessage = queue.pull();
		
		assertEquals(msgBody, actualMessage.getBody());
		assertEquals(36, actualMessage.getReceiptHandle().length());
		assertEquals(0, queue.getApproximateNumberOfMessages());
		assertEquals(1, queue.getNumberOfInflightMessages());
	}
	
	@Test
    public void testPullMessageForEmptyQueue() {
		Message actualMessage = queue.pull();
		
		assertNull(actualMessage);
	}
	
	@Test
    public void testDeleteMessageWithValidReceiptHandle() {
		String msgBody = "Hello Queue!";
		Message message = MessageCreator.createMessage(msgBody);
		
		queue.push(message);
		Message receivedMessage = queue.pull();
		
		boolean status = queue.delete(receivedMessage.getReceiptHandle());
		assertTrue(status);
		assertEquals(0, queue.getApproximateNumberOfMessages());
		assertEquals(0, queue.getNumberOfInflightMessages());
	}
	
	@Test
    public void testDeleteMessageWithInvalidReceiptHandle() {
		String expectedError = "ReceiptHandle does not exist.";
		
		boolean deleteStatus = queue.delete(String.valueOf(UUID.randomUUID()));
		assertFalse(deleteStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);	
	}
	
	@Test
    public void testPurgeForNonEmptyAvailableMessageQueueEmptyInflightMessageQueue() {
		String msgBody1 = "Hello Queue!";
		String msgBody2 = "Hello Queue!";
		Message message1 = MessageCreator.createMessage(msgBody1);
		Message message2 = MessageCreator.createMessage(msgBody2);
		
		queue.push(message1);
		queue.push(message2);
		
		boolean purgeStatus = queue.purge();
		
		assertTrue(purgeStatus);
		assertEquals(0, queue.getApproximateNumberOfMessages());
		assertEquals(0, queue.getNumberOfInflightMessages());
	}
	
	@Test
	public void testPurgeForNonEmptyAvailableMessageQueueNonEmptyInflightMessageQueue() {
		String msgBody1 = "Hello Queue!";
		String msgBody2 = "Hello Queue!";
		Message message1 = MessageCreator.createMessage(msgBody1);
		Message message2 = MessageCreator.createMessage(msgBody2);
		
		queue.push(message1);
		queue.push(message2);
		
		Message receivedMessage = queue.pull();
		
		boolean purgeStatus = queue.purge();
		
		assertTrue(purgeStatus);
		assertEquals(0, queue.getApproximateNumberOfMessages());
		assertEquals(0, queue.getNumberOfInflightMessages());
	}
	
	@Test
	public void testPurgeForEmptyAvailableMessageQueueNonEmptyInflightMessageQueue() {
		String msgBody1 = "Message 1";
		String msgBody2 = "Message 2";
		Message message1 = MessageCreator.createMessage(msgBody1);
		Message message2 = MessageCreator.createMessage(msgBody2);
		
		queue.push(message1);
		queue.push(message2);
		
		Message receivedMessage1 = queue.pull();
		Message receivedMessage2 = queue.pull();
		
		boolean purgeStatus = queue.purge();
		
		assertTrue(purgeStatus);
		assertEquals(0, queue.getApproximateNumberOfMessages());
		assertEquals(0, queue.getNumberOfInflightMessages());
	}
	
	@Test
    public void testRefreshQueueForExpiredVisibilityTimeout() {
		String msgBody1 = "Message 1";
		String msgBody2 = "Message 2";
		Message message1 = MessageCreator.createMessage(msgBody1);
		Message message2 = MessageCreator.createMessage(msgBody2);
		
		queue.push(message1);
		queue.push(message2);
		
		Message receivedMessage1 = queue.pull();
		Message receivedMessage2 = queue.pull();
		
		Map<String, String> attributes = new HashMap<>();
		attributes.put("VisibilityTimeout", "0");
		queue.setAttributes(attributes);
		queue.refreshQueue();
		
		assertEquals(0, queue.getNumberOfInflightMessages());
		assertEquals(2, queue.getApproximateNumberOfMessages());
	}
	
	@Test
    public void testRefreshQueueForUnexpiredVisibilityTimeout() {
		String msgBody1 = "Message 1";
		String msgBody2 = "Message 2";
		Message message1 = MessageCreator.createMessage(msgBody1);
		Message message2 = MessageCreator.createMessage(msgBody2);
		
		queue.push(message1);
		queue.push(message2);
		
		Message receivedMessage1 = queue.pull();
		Message receivedMessage2 = queue.pull();
		
		Map<String, String> attributes = new HashMap<>();
		attributes.put("VisibilityTimeout", "30");
		queue.setAttributes(attributes);
		
		queue.refreshQueue();
		
		assertEquals(2, queue.getNumberOfInflightMessages());
		assertEquals(0, queue.getApproximateNumberOfMessages());
	}
	
	@Test
    public void testSetAttributeWithNullAttributes() {
		String expectedWarning = String.format("No attributes to set for queue %s", qName);
		queue.setAttributes(null);
		verifyLoggedMessage(expectedWarning, Level.WARN);
	}
	
	@Test
    public void testSetAttributeWithValidAttributeNameValidValue() {
		Map<String, String> attributes = new HashMap<>();
		attributes.put(validAttributeName, validAttributeMinValue);
		queue.setAttributes(attributes);
		
		Map<String, String> receivedAttributes = queue.getAttributes();
		assertTrue(receivedAttributes.containsKey(validAttributeName));
		assertEquals(validAttributeMinValue, receivedAttributes.get(validAttributeName));
	}
	
	@Test
    public void testSetAttributeWithValidAttributeNameInvalidValue() {
		Map<String, String> attributes = new HashMap<>();
		attributes.put(validAttributeName, validAttributeInvalidValue);
		String expectedError = String.format("Invalid attribute value (%s) found while setting attributes for queue %s", validAttributeInvalidValue, qName);
		
		queue.setAttributes(attributes);
		verifyLoggedMessage(expectedError, Level.ERROR);
		Map<String, String> receivedAttributes = queue.getAttributes();
		assertTrue(receivedAttributes.containsKey(validAttributeName));
		assertEquals(validAttributeDefaultValue, receivedAttributes.get(validAttributeName));
	}
	
	@Test
    public void testSetAttributeWithInvalidAttributeName() {
		Map<String, String> attributes = new HashMap<>();
		attributes.put(invalidAttributeName, validAttributeDefaultValue);
		String expectedError = String.format("Invalid attribute name (%s) found while setting attributes for queue %s", invalidAttributeName, qName);
		
		queue.setAttributes(attributes);
		verifyLoggedMessage(expectedError, Level.ERROR);
		Map<String, String> receivedAttributes = queue.getAttributes();
		assertFalse(receivedAttributes.containsKey(invalidAttributeName));
	}
	
	private void verifyLoggedMessage(String expectedMessage, Level expectedLevel) {
		verify(mockAppender).doAppend(captorLoggingEvent.capture());
		LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(expectedLevel));
        assertThat(loggingEvent.getRenderedMessage(), is(expectedMessage));
	}
		
}
