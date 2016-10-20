package com.example;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.Message;

public class InMemoryQueue implements MessageQueue{

	private String name;
	// queue specific attributes, such as visibility timeout
	private Map<String, String> attributes;
	
	// messages which are available for processing
	private LinkedList<Message> availableMessages;
	// inflight messages are kept in a separate map so that the object can be accessed based on the receipt handle in O(1) time
	private Map<String, Message> inflightMessageObjectMap;
	// <ReceiptHandle, LatestReceiptTime> pairs for inflight messages
	private Map<String, Long> inflightMessageReceiptTimeMap;
	private Queue<Message> inflightMessages; 
	
	private Timer refreshQueueTimer;
	// The timer is called at regular interval
	private long interval = 100L;
	
	QueueAttributeValidator attributesValidator;
	private final static Logger logger = Logger.getLogger(InMemoryQueue.class);
	
	public InMemoryQueue(String qName, QueueAttributeValidator attributesValidator) {
		this.name = qName;
		this.attributes = new HashMap<>();
		
		this.attributesValidator = attributesValidator;
		populateDefaultAttributes();
		
		availableMessages = new LinkedList<>();
		inflightMessages = new LinkedList<>();
		inflightMessageObjectMap = new HashMap<>();
		inflightMessageReceiptTimeMap = new HashMap<>();
		
		setRefreshQueueTimer();
	}

	private void setRefreshQueueTimer() {
		long delay = 100L;
		refreshQueueTimer = new Timer();
		refreshQueueTimer.scheduleAtFixedRate(new ScheduledTask(this), delay, interval);
	}
	
	public synchronized void push(Message message) {
		try{
			availableMessages.addLast(message);
		}catch(Exception e) {
			logger.error(String.format("Exception while adding message in queue: %s", name), e);
			message.setMessageId(null);
		}finally{
			notifyAll();
		}
	}
	
	// pull method extracts the message from the available queue and puts it in the inflight queue.
	// also keeps the <receipthandle, receipttime> pair and <receipthandle, message> pair in maps 
	// for O(1) delete operation.
	public synchronized Message pull() {
		Message message = null;
		
		try{
			if(availableMessages.size() > 0) {
				message = availableMessages.removeFirst();
				message.setReceiptHandle(String.valueOf(UUID.randomUUID()));
				inflightMessageObjectMap.put(message.getReceiptHandle(), message);
				inflightMessageReceiptTimeMap.put(message.getReceiptHandle(), System.currentTimeMillis());
				inflightMessages.offer(message);
			}
		}catch(Exception e) {
			logger.error(String.format("Exception caught while pulling message from queue %s", name), e);
		} finally{
			notifyAll();
		}
			
		return message;
	}

	// delete operation gets the message object from the inflightMessageObjectMap and removes this object 
	// from inflightQueue in O(1) time.
	public synchronized boolean delete(String receiptHandle) {
		boolean status = false;
		
		try{
			if(inflightMessageObjectMap.containsKey(receiptHandle)) {
				Message message = inflightMessageObjectMap.get(receiptHandle);
				inflightMessages.remove( message );
				inflightMessageObjectMap.remove(receiptHandle);
				inflightMessageReceiptTimeMap.remove(receiptHandle);
				status = true;
			}
			else {
				logger.error("ReceiptHandle does not exist.");
			}
		}catch(Exception e) {
			logger.error(String.format("Exception caught while deleting message from queue %s", name), e);
		}finally{
			notifyAll();
		}
		
		return status;
	}

	@Override
	public int getApproximateNumberOfMessages() {
		return availableMessages.size();
	}

	// refreshQueue activates the timed-out messages for reprocessing. For simplicity, it depends on 
	// the queue specific VisibilityTimeout, not the message specific attribute.
	@Override
	public synchronized void refreshQueue() {
		long visibilityTimeout = Integer.valueOf(attributes.get("VisibilityTimeout")) * 1000L;
		
		int index = 0;
		
		try{
			while(!inflightMessages.isEmpty()) {
				String receiptHandle = inflightMessages.peek().getReceiptHandle();
				long receiptTime = inflightMessageReceiptTimeMap.get(receiptHandle);
				
				if(System.currentTimeMillis() - receiptTime >= visibilityTimeout) {
					Message message = inflightMessages.poll();
					inflightMessageObjectMap.remove(message.getReceiptHandle());
					inflightMessageReceiptTimeMap.remove(message.getReceiptHandle());
					
					message.setReceiptHandle(null);
					availableMessages.add(index, message); 
					index++;
				}
				else 
					break;
			}
		}catch(Exception e) {
			logger.error(String.format("Exception caught while refreshing queue %s", name), e);
		}finally{
			notifyAll();
		}
		
	}

	@Override
	public synchronized boolean purge() {
		
		try{
			availableMessages.clear();
			inflightMessageObjectMap.clear();
			inflightMessageReceiptTimeMap.clear();
			inflightMessages.clear();
			
		}catch(Exception e) {
			logger.error(String.format("Execption caught while purging queue %s", name), e);
			return false;
		}finally{
			notifyAll();
		}
		
		return true;
	}

	@Override
	public synchronized void setAttributes(Map<String, String> attributes) {
		if(attributes == null || attributes.size() == 0) {
			logger.warn(String.format("No attributes to set for queue %s", name));
			return;
		}
		
		for(Map.Entry<String, String> entry: attributes.entrySet()) {
			if(attributesValidator.validateAttributeName(entry.getKey())){ 
				if(attributesValidator.validateAttributeValue(entry.getKey(), entry.getValue()))
					this.attributes.put(entry.getKey(), entry.getValue());
				else {
					this.attributes.put(entry.getKey(), attributesValidator.getDefaultValue(entry.getKey()));
					logger.error(String.format("Invalid attribute value (%s) found while setting attributes for queue %s", entry.getValue(), name));
				}
			}
			else {
				logger.error(String.format("Invalid attribute name (%s) found while setting attributes for queue %s", entry.getKey(), name));
			}
		}
		
		notifyAll();
	}

	@Override
	public Map<String, String> getAttributes() {
		return attributes;
	}
	
	private void populateDefaultAttributes() {
		attributes = attributesValidator.getDefaultAttributeValues();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getNumberOfInflightMessages() {
		return inflightMessages.size();
	}

	@Override
	public synchronized void releaseResources() {
		refreshQueueTimer.cancel();
		notifyAll();	
	}
  
}
