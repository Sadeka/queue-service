package com.example;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

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
public class VisibilityTimeoutValidatorTest {
	VisibilityTimeoutValidator visibilityTimeoutValidator = new VisibilityTimeoutValidator();
	
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
	public void testValidateForLessThanMinValue() {
		String value = "-40";
		String expectedError = String.format("Invalid value - lies outside the acceptable range %s", value);
		
		boolean validationStatus = visibilityTimeoutValidator.validate(value);
		assertFalse(validationStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testValidateForMinValue() {
		String value = "0";
		
		boolean validationStatus = visibilityTimeoutValidator.validate(value);
		assertTrue(validationStatus);
	}
	
	@Test
	public void testValidateForGreaterThanMaxValue() {
		String value = "80000";
		String expectedError = String.format("Invalid value - lies outside the acceptable range %s", value);
		
		boolean validationStatus = visibilityTimeoutValidator.validate(value);
		assertFalse(validationStatus);
		verifyLoggedMessage(expectedError, Level.ERROR);
	}
	
	@Test
	public void testValidateForMaxValue() {
		String value = "43200";
		
		boolean validationStatus = visibilityTimeoutValidator.validate(value);
		assertTrue(validationStatus);
	}
	
	@Test
	public void testValidateForWithinRangeValue() {
		String value = "10000";
		
		boolean validationStatus = visibilityTimeoutValidator.validate(value);
		assertTrue(validationStatus);
	}
	
	private void verifyLoggedMessage(String expectedMessage, Level expectedLevel) {
		verify(mockAppender).doAppend(captorLoggingEvent.capture());
		LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(expectedLevel));
        assertThat(loggingEvent.getRenderedMessage(), is(expectedMessage));
	}
}
