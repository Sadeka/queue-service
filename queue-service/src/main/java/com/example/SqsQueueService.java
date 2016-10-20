package com.example;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.StringUtils;

public class SqsQueueService implements QueueService {
  
	// An adapter for AWS backed queue service - SQS; implemented the push method only. 
	
	private AmazonSQSClient sqsClient;
	private final static Logger logger = Logger.getLogger(SqsQueueService.class);
	
	public SqsQueueService(AmazonSQSClient sqsClient) {
	  this.sqsClient = sqsClient;
	}

	@Override
	public boolean createQueue(CreateQueueRequest createQueueRequest) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean deleteQueue(String qName) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public List<String> getQueueNames() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void setQueueAttributes(String qName, Map<String, String> attributes) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Map<String, String> getQueueAttributes(String qName) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int getApproximateNumberOfMessages(String qName) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean purgeQueue(String qName) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String push(String qName, String messageBody) {
		String queueUrl = null;
		String messageId = null;
		
		if(StringUtils.isNullOrEmpty(qName)) {
			logger.error(String.format("Queue name is null or empty. Failed to push message into %s", qName));
			return messageId;
		}
		
		try{
			GetQueueUrlResult getQueueUrlResult = sqsClient.getQueueUrl(qName);
			queueUrl = getQueueUrlResult.getQueueUrl();
		}catch(AmazonServiceException e) {
			logger.error(String.format("Failed to get a valid queue url for queue name %s. Error code:%s, Error type: %s, Error message: %s ", qName, e.getErrorCode(), e.getErrorType(), e.getErrorMessage()));
		}catch (AmazonClientException e) {
	        logger.error(String.format("Unable to get message queue %s", qName), e);
	    }
		
		if(StringUtils.isNullOrEmpty(queueUrl))
			return messageId;
		
		if(StringUtils.isNullOrEmpty(messageBody)) {
			logger.error(String.format("Message body is null or empty. Falied to push the message into queue %s",qName));
			return messageId;
		}
		
		try{
			SendMessageResult sendMessageResult = sqsClient.sendMessage(queueUrl, messageBody);
			messageId = sendMessageResult.getMessageId();
		}catch(AmazonServiceException e) {
			logger.error(String.format("Failed to push message into the queue %s. Error code:%s, Error type: %s, Error message: %s ", qName, e.getErrorCode(), e.getErrorType(), e.getErrorMessage()));
		}catch (AmazonClientException e) {
	        logger.error(String.format("Unable to reach message queue %s", qName), e);
	    }
		
		return messageId;
	}
	
	@Override
	public Message pull(String qName) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean delete(String qName, String receiptHandle) {
		throw new UnsupportedOperationException();
	}
}
