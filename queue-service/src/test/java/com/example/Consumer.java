package com.example;

import com.amazonaws.services.sqs.model.Message;

// consumer consumes message
public class Consumer implements Runnable{
	private final QueueService queueService;
	private final String qName;
	private String name;
	
	public Consumer(String name, QueueService queueService, String qName) {
		this.name = name;
		this.queueService = queueService;
		this.qName = qName;
	}

	@Override
	public void run() {
		while(!Thread.interrupted()) {
			try{
				Message message = queueService.pull(qName);
				
				if(message != null) {
					System.out.println(name + " pulled message: " + message.getReceiptHandle());
					Thread.sleep(100);
					
					if(queueService.delete(qName, message.getReceiptHandle()))
						System.out.println(name + " deleted message: " + message.getReceiptHandle());
				}
				
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}
