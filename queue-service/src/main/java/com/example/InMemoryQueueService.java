package com.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.StringUtils;

public class InMemoryQueueService implements QueueService {
	private ConcurrentMap<String, MessageQueue> queueMap;
	private QueueFactory queueFactory;
	private final static Logger logger = Logger.getLogger(InMemoryQueueService.class);
	
	public InMemoryQueueService(QueueFactory queueFactory) {
		queueMap = new ConcurrentHashMap<>();
		this.queueFactory = queueFactory;
	}
	
	public boolean createQueue(CreateQueueRequest createQueueRequest) {
		if(createQueueRequest == null) {
			logger.error("Queue request is null. Failed to create a queue.");
			return false;
		}
		
		String qName = createQueueRequest.getQueueName();
				
		if(StringUtils.isNullOrEmpty(qName)) {
			logger.error("Queue name is null or empty. Failed to create a queue.");
			return false;
		}
		
		if(queueMap.containsKey(qName)) {
			logger.error(String.format("Queue name already exists: %s", qName));
			return false;
		}
		
		MessageQueue queue = queueFactory.create(createQueueRequest);
		
		if(queue == null)
			return false;
		
		try{
			queueMap.put(qName, queue);
		}catch(Exception e) {
			logger.error(String.format("Exception caught while creating queue %s", qName), e);
			return false;
		}
		
		return true;
	}
	
	public boolean deleteQueue(String qName) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to delete the queue %s", qName));
			return false;
		}
		
		try{
			MessageQueue queue = getQueue(qName);
			queue.releaseResources();
			queueMap.remove(qName);
		}catch(Exception e) {
			logger.error(String.format("Exception caught while deleting queue %s", qName), e);
			return false;
		}
		
		return true;
	}

	private boolean isValidQueueName(String qName) {
		return !StringUtils.isNullOrEmpty(qName) && queueMap.containsKey(qName);
	}
	
	public List<String> getQueueNames() {
		List<String> queueNames = new ArrayList<>(queueMap.keySet());
		
		return queueNames;
	}
	
	@Override
	public String push(String qName, String messageBody) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to push message into %s", qName));
			return null;
		}
		
		if(StringUtils.isNullOrEmpty(messageBody)) {
			logger.error(String.format("Message body is null or empty. Failed to push message into queue %s",qName));
			return null;
		}
		
		MessageQueue queue = getQueue(qName);
		Message message = MessageCreator.createMessage(messageBody);
		queue.push(message);
		
		return message.getMessageId();
	}

	private MessageQueue getQueue(String qName) {
		return queueMap.get(qName);
	}

	@Override
	public Message pull(String qName) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to pull message from %s", qName));
			return null;
		}
		
		MessageQueue queue = getQueue(qName);
		return queue.pull();
	}

	@Override
	public boolean delete(String qName, String receiptHandle) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to delete message from %s", qName));
			return false;
		}
		
		MessageQueue queue = getQueue(qName);
		return queue.delete(receiptHandle);
	}

	@Override
	public int getApproximateNumberOfMessages(String qName) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to count messages in %s", qName));
			return -1;
		}
		
		MessageQueue queue = getQueue(qName);
		return queue.getApproximateNumberOfMessages();
	}

	@Override
	public void setQueueAttributes(String qName, Map<String, String> attributes) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to set attributes of %s", qName));
			return;
		}
		
		MessageQueue queue = getQueue(qName);
		queue.setAttributes(attributes);
	}

	@Override
	public Map<String, String> getQueueAttributes(String qName) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to get attributes of %s", qName));
			return null;
		}
		
		MessageQueue queue = getQueue(qName);
		return queue.getAttributes();
		
	}

	@Override
	public boolean purgeQueue(String qName) {
		if(!isValidQueueName(qName)) {
			logger.error(String.format("Queue name not found. Failed to purge queue %s", qName));
			return false;
		}
		
		MessageQueue queue = getQueue(qName);
		return queue.purge();
	}
  
}
