package com.example;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.Message;

public class MessageCreator {
	private static final String encoding = "UTF-8";
	private static final String messageDigestAlgorithm = "MD5";
	private static final Logger logger = Logger.getLogger(MessageCreator.class);
	
	public static Message createMessage(String messageBody) {
		Message message = null;
		String digestOfBody = generateDigest(messageBody);
		
		if(digestOfBody != null) {
			message = new Message();
			message.setMessageId(String.valueOf(UUID.randomUUID()));
			message.setBody(messageBody);
		}
		
		return message;
	}

	private static String generateDigest(String str) {
		String digestedString = null;
		MessageDigest messageDigest = null;
		
		try {
			messageDigest = MessageDigest.getInstance(messageDigestAlgorithm);
		} catch (NoSuchAlgorithmException e) {
			logger.error("Failed to get an instance of message digest", e);
		}
		
		if(messageDigest != null) {
			try {
				byte[] bytesOfMessageBody = str.getBytes(encoding);
				byte[] theDigest = messageDigest.digest(bytesOfMessageBody);
				digestedString = String.valueOf(theDigest);
			} catch (UnsupportedEncodingException e) {
				logger.error("Unsupported encoding found while digesting message.", e);
			}
		}
		
		return digestedString;
	}
}
