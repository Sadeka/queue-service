package com.example;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

@RunWith(MockitoJUnitRunner.class)
public class QueueAttributeValidatorImplTest {
	private QueueAttributeValidator queueAttributeValidator = new QueueAttributeValidatorImpl();
	private String validAttributeName = "VisibilityTimeout";
	private String invalidAttributeName = "GeneralTimeout";
	
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
		LogManager.getRootLogger().addAppender(mockAppender);
	}
	
	@Test
	public void testValidateAttributeNameForValidAttributeName() {
		boolean validationStatus = queueAttributeValidator.validateAttributeName(validAttributeName);
		assertTrue(validationStatus);
	}
	
	@Test
	public void testValidateAttributeNameForInvalidAttributeName() {
		boolean validationStatus = queueAttributeValidator.validateAttributeName(invalidAttributeName);
		assertFalse(validationStatus);
	}
	
	@Test
	public void testValidateAttributeValueForValidAttributeNameValidValue() {
		String validValue = "60";
		boolean mockedStatus = true;
		AttributeValidator mockAttributeValidator = mock(AttributeValidator.class);
		when(mockAttributeValidator.validate(validValue)).thenReturn(mockedStatus);
		
		boolean validationStatus = queueAttributeValidator.validateAttributeValue(validAttributeName, validValue);
		assertTrue(validationStatus);
	}
	
	@Test
	public void testValidateAttributeValueForValidAttributeNameInvalidValue() {
		String invalidValue = "80000";
		boolean mockedStatus = false;
		AttributeValidator mockAttributeValidator = mock(AttributeValidator.class);
		when(mockAttributeValidator.validate(invalidValue)).thenReturn(mockedStatus);
		
		boolean validationStatus = queueAttributeValidator.validateAttributeValue(validAttributeName, invalidValue);
		assertFalse(validationStatus);
	}
	
	@Test
	public void testValidateAttributeValueForInvalidAttributeName() {
		String expectedError = String.format("Invalid attribute %s", invalidAttributeName);
		String value = "80";
		
		boolean validationStatus = queueAttributeValidator.validateAttributeValue(invalidAttributeName, value);
		assertFalse(validationStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testGetDefaultValueForValidAttributeName() {
		String mockedValue = "30";
		AttributeValidator mockAttributeValidator = mock(AttributeValidator.class);
		when(mockAttributeValidator.getDefault()).thenReturn(mockedValue);
		
		String receivedValue = queueAttributeValidator.getDefaultValue(validAttributeName);
		assertEquals(mockedValue, receivedValue);
	}
	
	@Test
	public void testGetDefaultValueForInvalidAttributeName() {
		String expectedError = String.format("Invalid attribute %s", invalidAttributeName);
		AttributeValidator mockAttributeValidator = mock(AttributeValidator.class);
		
		String receivedValue = queueAttributeValidator.getDefaultValue(invalidAttributeName);
		assertNull(receivedValue);
		verify(mockAttributeValidator, never()).getDefault();
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	private void verifyLoggedMessage(String expectedMessage, Level expectedLevel) {
		verify(mockAppender).doAppend(captorLoggingEvent.capture());
		LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(expectedLevel));
        assertThat(loggingEvent.getRenderedMessage(), is(expectedMessage));
	}

}
